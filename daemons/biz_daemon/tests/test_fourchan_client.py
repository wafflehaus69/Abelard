"""fourchan_client: HTML cleaning, throttle, 304, discovery, loud-fail."""

from __future__ import annotations

import pytest

from biz_daemon import fourchan_client as fc
from biz_daemon.fourchan_client import Fetcher, FourchanError, NoSmgThreadError


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None, malformed=False):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self._malformed = malformed

    def json(self):
        if self._malformed:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    """Returns queued responses in order; records requested URLs + headers."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append((url, dict(headers or {})))
        return self._responses.pop(0)


# --- clean_com ---------------------------------------------------------------


def test_clean_com_strips_br_to_newline():
    assert fc.clean_com("line1<br>line2") == "line1\nline2"


def test_clean_com_drops_quotelinks_entirely():
    raw = '<a href="#p123" class="quotelink">&gt;&gt;123</a> based take'
    assert fc.clean_com(raw) == "based take"


def test_clean_com_keeps_greentext_text_drops_wrapper():
    raw = '<span class="quote">&gt;he actually bought</span>'
    assert fc.clean_com(raw) == ">he actually bought"


def test_clean_com_decodes_entities():
    assert fc.clean_com("AT&amp;T to $30 &#039;soon&#039;") == "AT&T to $30 'soon'"


def test_clean_com_strips_wbr_rejoining_token():
    assert fc.clean_com("BAGHO<wbr>LDER") == "BAGHOLDER"


def test_clean_com_image_only_post_is_empty():
    assert fc.clean_com(None) == ""
    assert fc.clean_com("") == ""


# --- throttle ----------------------------------------------------------------


def test_throttle_enforces_min_interval():
    times = iter([100.0, 100.2, 101.0])
    sleeps = []
    session = FakeSession(
        [FakeResponse(200, {"a": 1}), FakeResponse(200, {"b": 2})]
    )
    fetcher = Fetcher(
        user_agent="t",
        session=session,
        sleep=sleeps.append,
        clock=lambda: next(times),
    )
    fetcher.get_json("https://a.4cdn.org/biz/one.json")
    fetcher.get_json("https://a.4cdn.org/biz/two.json")
    assert sleeps == [pytest.approx(0.8)]


# --- conditional request / 304 ----------------------------------------------


def test_304_returns_cached_payload_not_an_error():
    url = "https://a.4cdn.org/biz/thread/1.json"
    session = FakeSession(
        [
            FakeResponse(200, {"posts": [{"no": 1}]}, headers={"Last-Modified": "GMT"}),
            FakeResponse(304),
        ]
    )
    # constant clock so throttle never sleeps in this test
    fetcher = Fetcher(
        user_agent="t", session=session, sleep=lambda _s: None, clock=lambda: 0.0
    )
    first = fetcher.get_json(url)
    second = fetcher.get_json(url)
    assert first == second == {"posts": [{"no": 1}]}
    # second request carried the If-Modified-Since header
    assert session.calls[1][1].get("If-Modified-Since") == "GMT"


def test_non_200_is_loud_fail():
    session = FakeSession([FakeResponse(500)])
    fetcher = Fetcher(
        user_agent="t", session=session, sleep=lambda _s: None, clock=lambda: 0.0
    )
    with pytest.raises(FourchanError):
        fetcher.get_json("https://a.4cdn.org/biz/catalog.json")


def test_malformed_json_is_loud_fail():
    session = FakeSession([FakeResponse(200, malformed=True)])
    fetcher = Fetcher(
        user_agent="t", session=session, sleep=lambda _s: None, clock=lambda: 0.0
    )
    with pytest.raises(FourchanError):
        fetcher.get_json("https://a.4cdn.org/biz/catalog.json")


# --- discovery ---------------------------------------------------------------


def _fetcher_with(responses):
    return Fetcher(
        user_agent="t",
        session=FakeSession(responses),
        sleep=lambda _s: None,
        clock=lambda: 0.0,
    )


def test_discovery_matches_smg_subject_case_insensitive():
    catalog = [
        {"page": 1, "threads": [
            {"no": 100, "sub": "/SMG/ - Stock Market General"},
            {"no": 101, "sub": "Crypto general"},
        ]},
        {"page": 2, "threads": [
            {"no": 102, "com": "/smg/ in the body, not subject"},
        ]},
    ]
    fetcher = _fetcher_with([FakeResponse(200, catalog)])
    matches = fc.discover_smg_thread_nos(fetcher)
    assert matches == [(100, "/SMG/ - Stock Market General")]


def test_zero_smg_threads_raises_explicit_state():
    catalog = [{"page": 1, "threads": [{"no": 1, "sub": "biz general"}]}]
    fetcher = _fetcher_with([FakeResponse(200, catalog)])
    with pytest.raises(NoSmgThreadError):
        fc.scrape_smg(fetcher)


# --- UTF-8 decode regression (encoding bug) ---------------------------------

import json as _json  # noqa: E402

import requests as _requests  # noqa: E402

from biz_daemon import extractor as _extractor  # noqa: E402


def _utf8_response(obj, *, status=200, encoding="ISO-8859-1"):
    """A real requests.Response holding UTF-8 bytes but a MIS-inferred encoding.

    Simulates a.4cdn.org's UTF-8 JSON when requests guesses the wrong encoding
    (latin-1 / cp1252 / None). Our client must force utf-8 regardless.
    """
    r = _requests.Response()
    r.status_code = status
    r._content = _json.dumps(obj, ensure_ascii=False).encode("utf-8")
    r.encoding = encoding
    r.headers["Content-Type"] = "application/json"
    return r


# en-dash subject + tickers wedged against non-ASCII punctuation/accents
_NONASCII_CATALOG = [
    {"page": 1, "threads": [{"no": 700, "sub": "/smg/ – Stock Market General"}]}
]
_NONASCII_THREAD = {"posts": [
    {"no": 700, "sub": "/smg/ – Stock Market General", "com": "buying “GME” today"},
    {"no": 701, "com": "NVDA—to the moon"},          # em-dash flush against ticker
    {"no": 702, "com": "café vibes, AMD – strong"},  # accent + en-dash
    {"no": 703, "com": "it’s TSLA season"},          # smart apostrophe
]}


def _utf8_fetcher(responses):
    return Fetcher(
        user_agent="t",
        session=FakeSession(responses),
        sleep=lambda _s: None,
        clock=lambda: 0.0,
    )


def test_response_decoded_utf8_regardless_of_headers():
    for enc in ("ISO-8859-1", None, "cp1252"):
        fetcher = _utf8_fetcher([_utf8_response(_NONASCII_THREAD, encoding=enc)])
        data = fetcher.get_json("https://a.4cdn.org/biz/thread/700.json")
        assert data["posts"][0]["com"] == "buying “GME” today"
        assert "—" in data["posts"][1]["com"]   # em-dash intact
        assert "é" in data["posts"][2]["com"]   # é intact, not mojibake


def test_nonascii_subject_clean_and_tickers_extract():
    fetcher = _utf8_fetcher(
        [_utf8_response(_NONASCII_CATALOG), _utf8_response(_NONASCII_THREAD)]
    )
    threads = fc.scrape_smg(fetcher)
    t = threads[0]

    # subject parses clean: real en-dash, not "ΓÇô" / "Ã" mojibake
    assert t.subject == "/smg/ – Stock Market General"
    assert "–" in t.subject
    assert "Ã" not in t.subject and "Γ" not in t.subject

    # cleaner preserved the Unicode in bodies
    assert "–" in t.posts[2]["com"]

    # every ticker adjacent to non-ASCII still extracts (the regression)
    universe = frozenset({"GME", "NVDA", "AMD", "TSLA"})
    table = _extractor.extract(t.posts, universe=universe, blacklist=frozenset())
    assert set(table) == {"GME", "NVDA", "AMD", "TSLA"}


def test_scrape_smg_collects_posts_and_cleans_com():
    catalog = [{"page": 1, "threads": [{"no": 100, "sub": "/smg/ general"}]}]
    thread = {"posts": [
        {"no": 100, "sub": "/smg/ general", "com": "buy <b>GME</b>"},
        {"no": 101, "com": '<a class="quotelink">&gt;&gt;100</a> sell'},
        {"no": 102},  # image-only
    ]}
    fetcher = _fetcher_with([FakeResponse(200, catalog), FakeResponse(200, thread)])
    threads = fc.scrape_smg(fetcher)
    assert len(threads) == 1
    t = threads[0]
    assert t.no == 100
    assert t.post_count == 3
    assert t.posts[0]["com"] == "buy GME"
    assert t.posts[1]["com"] == "sell"
    assert t.posts[2]["com"] == ""
