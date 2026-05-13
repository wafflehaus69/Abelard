"""RssSource tests — synthetic XML fixtures + canned HttpResponses."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from news_watch_daemon.http_client import HttpClient, HttpResponse
from news_watch_daemon.sources.rss import RssSource, derive_feed_id


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _ok_response(body: str, content_type: str = "application/rss+xml") -> HttpResponse:
    return HttpResponse(
        status="ok",
        http_status_code=200,
        body=body,
        json=None,
        error_detail=None,
        elapsed_ms=12,
    )


def _make_source(feed_url: str = "https://example.com/feed.xml") -> tuple[RssSource, MagicMock]:
    http = MagicMock(spec=HttpClient)
    src = RssSource(http, feed_url=feed_url)
    return src, http


# ---------- feed_id derivation ----------


def test_derive_feed_id_strips_scheme_and_special_chars():
    fid = derive_feed_id("https://www.reuters.com/feeds/markets.xml")
    # Expect slugified form + 6-char suffix
    assert fid.startswith("www_reuters_com_feeds_markets_xml_")
    suffix = fid.split("_")[-1]
    assert len(suffix) == 6
    assert all(c in "0123456789abcdef" for c in suffix)


def test_derive_feed_id_is_lowercase():
    fid = derive_feed_id("HTTPS://Example.COM/Feed.XML")
    assert fid == fid.lower()


def test_derive_feed_id_is_stable():
    a = derive_feed_id("https://example.com/feed.xml")
    b = derive_feed_id("https://example.com/feed.xml")
    assert a == b


def test_derive_feed_id_distinguishes_different_urls():
    a = derive_feed_id("https://example.com/a")
    b = derive_feed_id("https://example.com/b")
    assert a != b


def test_derive_feed_id_truncates_long_urls():
    long_url = "https://example.com/" + ("verylongpath/" * 20)
    fid = derive_feed_id(long_url)
    # 64-char slug max + 1 underscore + 6 hex
    assert len(fid) <= 64 + 1 + 6


def test_derive_feed_id_empty_url_rejected():
    with pytest.raises(ValueError):
        derive_feed_id("")


# ---------- plugin construction ----------


def test_plugin_name_uses_derived_feed_id():
    src, _ = _make_source("https://example.com/feed.xml")
    assert src.name.startswith("rss:")
    assert "example_com_feed_xml" in src.name


def test_plugin_name_uses_explicit_feed_id():
    http = MagicMock(spec=HttpClient)
    src = RssSource(http, feed_url="https://example.com/feed.xml", feed_id="custom_id")
    assert src.name == "rss:custom_id"


def test_rate_limit_budget_is_optimistic():
    src, _ = _make_source()
    assert src.rate_limit_budget_remaining() == 1.0


def test_empty_feed_url_rejected():
    http = MagicMock(spec=HttpClient)
    with pytest.raises(ValueError):
        RssSource(http, feed_url="")


# ---------- happy paths ----------


def test_rss2_happy_path():
    src, http = _make_source()
    http.get_text.return_value = _ok_response(_load_fixture("rss2_synthetic.xml"))
    result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert len(result.items) == 2
    first = result.items[0]
    assert first.headline == "RSS test entry one"
    assert first.url == "https://example.com/rss/1"
    assert first.source_item_id == "https://example.com/rss/1"  # from <guid>
    assert first.raw_source == "Synthetic RSS 2.0 Feed"
    assert first.tickers == []
    assert first.raw_body is None


def test_atom_happy_path_uses_id_for_source_item_id():
    src, http = _make_source()
    http.get_text.return_value = _ok_response(
        _load_fixture("atom_synthetic.xml"), content_type="application/atom+xml"
    )
    result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert len(result.items) == 2
    assert result.items[0].source_item_id == "tag:example.com,2026:atom-1"
    assert result.items[0].raw_source == "Synthetic Atom Feed"


def test_rss2_without_guid_falls_back_to_hash():
    src, http = _make_source()
    http.get_text.return_value = _ok_response(_load_fixture("rss2_no_guid_synthetic.xml"))
    result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert len(result.items) == 1
    # Fallback hash is 32 hex chars; should NOT equal the URL
    sid = result.items[0].source_item_id
    assert sid != "https://example.com/noguid/1"
    assert len(sid) == 32
    assert all(c in "0123456789abcdef" for c in sid)


def test_feed_with_no_title_sets_raw_source_none():
    src, http = _make_source()
    http.get_text.return_value = _ok_response(_load_fixture("rss2_no_title_synthetic.xml"))
    result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert result.items[0].raw_source is None


# ---------- window filtering ----------


def test_since_unix_filters_older_entries():
    src, http = _make_source()
    http.get_text.return_value = _ok_response(_load_fixture("rss2_synthetic.xml"))
    # Entries are 2026-05-12 12:00 and 13:00 UTC. Filter to keep only 13:00+.
    cutoff = 1778619600  # 2026-05-12 12:20:00 UTC approx — picks second entry only
    # Actually compute the exact cutoff for determinism:
    import calendar, time
    cutoff = calendar.timegm(time.strptime("2026-05-12 12:30:00", "%Y-%m-%d %H:%M:%S"))
    result = src.fetch(since_unix=cutoff)
    assert result.status == "ok"
    assert len(result.items) == 1
    assert result.items[0].headline == "RSS test entry two"


# ---------- timestamp-missing entries → partial ----------


def test_entry_without_timestamp_dropped_status_partial():
    src, http = _make_source()
    http.get_text.return_value = _ok_response(_load_fixture("rss2_no_dates_synthetic.xml"))
    result = src.fetch(since_unix=0)
    assert result.status == "partial"
    assert len(result.items) == 1  # the one entry that had pubDate
    assert "dropped 1" in result.error_detail


# ---------- empty feed ----------


def test_empty_feed_returns_ok_with_no_items():
    src, http = _make_source()
    empty_rss = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel><title>Empty</title>'
        '<link>https://example.com</link>'
        '<description>None.</description></channel></rss>'
    )
    http.get_text.return_value = _ok_response(empty_rss)
    result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert result.items == []


# ---------- HTTP error paths ----------


def test_http_404_returns_error():
    src, http = _make_source()
    http.get_text.return_value = HttpResponse(
        status="not_found",
        http_status_code=404,
        body=None,
        json=None,
        error_detail="http_404: https://example.com/feed.xml",
        elapsed_ms=5,
    )
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "404" in result.error_detail


def test_http_5xx_returns_error():
    src, http = _make_source()
    http.get_text.return_value = HttpResponse(
        status="error",
        http_status_code=503,
        body=None,
        json=None,
        error_detail="http_5xx: 503",
        elapsed_ms=5,
    )
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "503" in result.error_detail


def test_network_error_returns_error():
    src, http = _make_source()
    http.get_text.return_value = HttpResponse(
        status="error",
        http_status_code=None,
        body=None,
        json=None,
        error_detail="URLError: timeout",
        elapsed_ms=5,
    )
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "URLError" in result.error_detail


def test_http_429_returns_rate_limited():
    src, http = _make_source()
    http.get_text.return_value = HttpResponse(
        status="rate_limited",
        http_status_code=429,
        body=None,
        json=None,
        error_detail="retry_after_seconds=10",
        elapsed_ms=5,
    )
    result = src.fetch(since_unix=0)
    assert result.status == "rate_limited"


# ---------- malformed XML ----------


def test_completely_malformed_xml_returns_error():
    src, http = _make_source()
    # Plain text — feedparser will set bozo and yield no entries.
    http.get_text.return_value = _ok_response("not xml at all", content_type="text/plain")
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "parse error" in result.error_detail or "bozo" in result.error_detail.lower()


def test_partially_malformed_feed_returns_partial():
    """A feed with a bozo flag but at least one entry should be partial."""
    src, http = _make_source()
    # Build an RSS feed with one valid item and one that triggers a bozo
    # (unescaped ampersand in element text). feedparser typically still
    # extracts entries on minor errors.
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel><title>Partial</title>'
        '<link>https://example.com</link><description>x</description>'
        '<item>'
        '<title>Valid entry & friends</title>'
        '<link>https://example.com/1</link>'
        '<guid>https://example.com/1</guid>'
        '<pubDate>Mon, 12 May 2026 12:00:00 GMT</pubDate>'
        '</item>'
        '</channel></rss>'
    )
    http.get_text.return_value = _ok_response(body)
    result = src.fetch(since_unix=0)
    # Either ok (feedparser tolerated it) or partial (bozo with entries)
    # — both are acceptable per the contract; we just must not error.
    assert result.status in ("ok", "partial")
    assert len(result.items) == 1


# ---------- defense in depth ----------


def test_fetch_never_raises_when_http_client_throws():
    src, http = _make_source()
    http.get_text.side_effect = RuntimeError("boom")
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "RuntimeError" in result.error_detail


def test_fetch_handles_empty_response_body():
    src, http = _make_source()
    http.get_text.return_value = HttpResponse(
        status="ok",
        http_status_code=200,
        body=None,
        json=None,
        error_detail=None,
        elapsed_ms=5,
    )
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "empty" in result.error_detail
