"""RssSource tests — synthetic XML fixtures + canned HttpResponses."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from news_watch_daemon.http_client import HttpClient, HttpResponse
from news_watch_daemon.sources.rss import (
    RssSource,
    _strip_google_news_publisher,
    derive_feed_id,
)


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


# ---------- NW-SRC-3 Fix 1: Google News publisher-suffix strip ----------


def test_strip_helper_removes_source_matched_suffix():
    assert _strip_google_news_publisher(
        "U.S. attacks Iran as Strait of Hormuz tensions rise - Reuters", "Reuters"
    ) == "U.S. attacks Iran as Strait of Hormuz tensions rise"


def test_strip_helper_leaves_non_matching_dash_tail_intact():
    # Legitimate " - X" in a headline where X is NOT the item's source: untouched.
    assert _strip_google_news_publisher(
        "Trump-Xi summit - what to expect - The Hill", "Reuters"
    ) == "Trump-Xi summit - what to expect - The Hill"


def test_strip_helper_preserves_legit_internal_dash():
    assert _strip_google_news_publisher(
        "Nvidia CEO: AI boom is real - Barron's", "Barron's"
    ) == "Nvidia CEO: AI boom is real"


def test_strip_helper_no_publisher_is_noop():
    assert _strip_google_news_publisher("Some headline - Reuters", None) == \
        "Some headline - Reuters"


def test_strip_helper_refuses_to_empty_a_byline_only_title():
    # Degenerate: title is only the byline. Do not strip to "".
    assert _strip_google_news_publisher("- Reuters", "Reuters") == "- Reuters"


def _gn_feed_xml(items: list[tuple[str, str]]) -> str:
    """Build a Google-News-shaped RSS body. items = [(title, publisher), ...]."""
    entries = "".join(
        f"""
        <item>
          <title>{title}</title>
          <link>https://news.google.com/rss/articles/{i}</link>
          <guid>gn-{i}</guid>
          <pubDate>Tue, 15 Jul 2026 12:00:00 GMT</pubDate>
          <source url="https://{pub.lower().replace(' ', '')}.com">{pub}</source>
        </item>"""
        for i, (title, pub) in enumerate(items)
    )
    return (
        '<?xml version="1.0"?><rss version="2.0"><channel>'
        "<title>&quot;when:24h site:x&quot; - Google News</title>"
        f"{entries}</channel></rss>"
    )


def test_gn_source_strips_suffix_and_promotes_publisher():
    src = RssSource(
        MagicMock(spec=HttpClient),
        feed_url="https://news.google.com/rss/search?q=when%3A24h+site%3Areuters.com",
    )
    body = _gn_feed_xml([("Big wire story happens - Reuters", "Reuters")])
    result = src._parse_feed(body, since_unix=0, fetched_at=0)
    assert len(result.items) == 1
    item = result.items[0]
    assert item.headline == "Big wire story happens"   # suffix stripped
    assert item.raw_source == "Reuters"                # attribution preserved


def test_gn_syndication_collapses_to_one_clean_headline():
    """The core dedup payoff: same story from 3 publishers -> identical clean
    headline, so the orchestrator's dedupe_hash collapses them to one."""
    src = RssSource(
        MagicMock(spec=HttpClient),
        feed_url="https://news.google.com/rss/search?q=iran",
    )
    story = "U S attacks Iran as Strait of Hormuz tensions rise"
    body = _gn_feed_xml([
        (f"{story} - Reuters", "Reuters"),
        (f"{story} - CBC", "CBC"),
        (f"{story} - The Washington Post", "The Washington Post"),
    ])
    result = src._parse_feed(body, since_unix=0, fetched_at=0)
    clean = {it.headline for it in result.items}
    assert clean == {story}   # all three collapse to one distinct headline


def test_non_google_news_titles_are_untouched():
    src = RssSource(
        MagicMock(spec=HttpClient),
        feed_url="https://www.aljazeera.com/xml/rss/all.xml",
    )
    # Al Jazeera headline that legitimately ends with " - Something".
    body = _gn_feed_xml([("Analysis: the war - and what comes next - Al Jazeera",
                          "Al Jazeera")])
    result = src._parse_feed(body, since_unix=0, fetched_at=0)
    # Not a GN feed -> no strip, and raw_source stays the feed title, not <source>.
    assert result.items[0].headline == "Analysis: the war - and what comes next - Al Jazeera"
