"""FinnhubGeneralNewsSource tests — synthetic fixture + canned HttpResponses.

The plugin's only collaborator is `HttpClient`. Tests inject a stub
`HttpClient` (`unittest.mock.MagicMock(spec=HttpClient)`) so plugin
logic is exercised without touching urllib. The shape we test against
matches what one real `/news?category=general` call returned (verified
2026-05-12); the synthetic fixture is identical in structure to the
real one but uses placeholder content.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from news_watch_daemon.http_client import HttpClient, HttpResponse
from news_watch_daemon.sources.finnhub_general import (
    FINNHUB_BASE_URL,
    FinnhubGeneralNewsSource,
    _parse_tickers,
)


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "finnhub_general_synthetic.json"


def _load_fixture() -> list[dict]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _ok_response(payload) -> HttpResponse:
    body = json.dumps(payload)
    return HttpResponse(
        status="ok",
        http_status_code=200,
        body=body,
        json=payload,
        error_detail=None,
        elapsed_ms=42,
    )


def _make_source(api_key: str | None = "k-test") -> tuple[FinnhubGeneralNewsSource, MagicMock]:
    http = MagicMock(spec=HttpClient)
    src = FinnhubGeneralNewsSource(http, api_key)
    return src, http


# ---------- plugin identity ----------


def test_plugin_name_is_stable():
    src, _ = _make_source()
    assert src.name == "finnhub:general"


def test_rate_limit_budget_is_optimistic():
    src, _ = _make_source()
    assert src.rate_limit_budget_remaining() == 1.0


# ---------- happy path against fixture ----------


def test_happy_path_returns_all_items_in_window():
    src, http = _make_source()
    fixture = _load_fixture()
    http.get_json.return_value = _ok_response(fixture)
    result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert result.source == "finnhub:general"
    assert result.error_detail is None
    assert len(result.items) == 3
    first = result.items[0]
    assert first.source_item_id == "100001"
    assert first.headline == "Test news item one"
    assert first.url == "https://example.com/news/1"
    assert first.published_at_unix == 1778600000
    assert first.raw_source == "TestWire"
    assert first.tickers == []
    assert first.raw_body is None


def test_happy_path_makes_correct_http_call():
    src, http = _make_source(api_key="abc-key")
    http.get_json.return_value = _ok_response([])
    src.fetch(since_unix=0)
    http.get_json.assert_called_once()
    args, kwargs = http.get_json.call_args
    assert args[0] == FINNHUB_BASE_URL
    assert kwargs["params"] == {"category": "general", "token": "abc-key"}


# ---------- empty / window filtering ----------


def test_empty_response_is_ok_with_no_items():
    src, http = _make_source()
    http.get_json.return_value = _ok_response([])
    result = src.fetch(since_unix=0)
    assert result.status == "ok"
    assert result.items == []


def test_since_unix_filters_older_items():
    src, http = _make_source()
    fixture = _load_fixture()
    # Fixture items have datetimes 1778600000, 1778603600, 1778607200.
    # since_unix=1778604000 should keep only the last one (1778607200).
    http.get_json.return_value = _ok_response(fixture)
    result = src.fetch(since_unix=1778604000)
    assert result.status == "ok"
    assert len(result.items) == 1
    assert result.items[0].source_item_id == "100003"


def test_window_filter_does_not_count_as_partial():
    """Items older than since_unix are filtered, not dropped — status stays ok."""
    src, http = _make_source()
    http.get_json.return_value = _ok_response(_load_fixture())
    result = src.fetch(since_unix=99_999_999_999)  # far future → all filtered
    assert result.status == "ok"
    assert result.items == []
    assert result.error_detail is None


# ---------- malformed items → partial ----------


def test_malformed_item_dropped_status_partial():
    src, http = _make_source()
    fixture = _load_fixture()
    # Add a malformed entry (no `id`) plus a list-with-no-headline.
    payload = fixture + [{"datetime": 1778610000, "headline": "no id"}]
    http.get_json.return_value = _ok_response(payload)
    result = src.fetch(since_unix=0)
    assert result.status == "partial"
    assert len(result.items) == 3
    assert "dropped 1" in result.error_detail


def test_item_with_invalid_datetime_dropped():
    src, http = _make_source()
    payload = [{"id": 1, "datetime": "not-a-number", "headline": "x"}]
    http.get_json.return_value = _ok_response(payload)
    result = src.fetch(since_unix=0)
    assert result.status == "partial"
    assert result.items == []
    assert "dropped 1" in result.error_detail


def test_item_with_empty_headline_dropped():
    src, http = _make_source()
    payload = [{"id": 1, "datetime": 1778600000, "headline": "  "}]
    http.get_json.return_value = _ok_response(payload)
    result = src.fetch(since_unix=0)
    assert result.status == "partial"
    assert result.items == []


# ---------- HTTP error mapping ----------


def test_http_429_returns_rate_limited():
    src, http = _make_source()
    http.get_json.return_value = HttpResponse(
        status="rate_limited",
        http_status_code=429,
        body=None,
        json=None,
        error_detail="retry_after_seconds=30",
        elapsed_ms=10,
    )
    result = src.fetch(since_unix=0)
    assert result.status == "rate_limited"
    assert result.items == []
    assert "retry_after_seconds=30" in result.error_detail


def test_http_500_returns_error():
    src, http = _make_source()
    http.get_json.return_value = HttpResponse(
        status="error",
        http_status_code=500,
        body=None,
        json=None,
        error_detail="http_5xx: 500",
        elapsed_ms=10,
    )
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "500" in result.error_detail


def test_network_failure_returns_error():
    src, http = _make_source()
    http.get_json.return_value = HttpResponse(
        status="error",
        http_status_code=None,
        body=None,
        json=None,
        error_detail="URLError: timeout",
        elapsed_ms=10,
    )
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "URLError" in result.error_detail


def test_unexpected_response_shape_returns_error():
    src, http = _make_source()
    # API returned a dict instead of a list — unexpected shape.
    http.get_json.return_value = _ok_response({"unexpected": "shape"})
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "expected list" in result.error_detail


# ---------- missing API key ----------


def test_missing_api_key_returns_error_without_http_call():
    src, http = _make_source(api_key=None)
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert result.error_detail == "FINNHUB_API_KEY not set"
    http.get_json.assert_not_called()


def test_empty_api_key_returns_error_without_http_call():
    src, http = _make_source(api_key="")
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    http.get_json.assert_not_called()


# ---------- defense in depth: never raise ----------


def test_fetch_never_raises_even_when_http_client_throws():
    src, http = _make_source()
    http.get_json.side_effect = RuntimeError("unexpected boom")
    result = src.fetch(since_unix=0)
    assert result.status == "error"
    assert "RuntimeError" in result.error_detail
    assert "unexpected boom" in result.error_detail


# ---------- ticker parsing ----------


def test_parse_tickers_empty_string():
    assert _parse_tickers("") == []


def test_parse_tickers_none():
    assert _parse_tickers(None) == []


def test_parse_tickers_single():
    assert _parse_tickers("AAPL") == ["AAPL"]


def test_parse_tickers_multi():
    assert _parse_tickers("AAPL,GOOGL,MSFT") == ["AAPL", "GOOGL", "MSFT"]


def test_parse_tickers_lowercases_uppercase():
    assert _parse_tickers("aapl,googl") == ["AAPL", "GOOGL"]


def test_parse_tickers_strips_whitespace():
    assert _parse_tickers("  AAPL ,  GOOGL  ") == ["AAPL", "GOOGL"]


def test_parse_tickers_drops_empties():
    assert _parse_tickers("AAPL,,GOOGL,") == ["AAPL", "GOOGL"]


def test_parse_tickers_preserves_dots_and_hyphens():
    """MOG.A, BRK.B, BF.A and similar share-class tickers must survive."""
    assert _parse_tickers("MOG.A,BRK.B,BF-A") == ["MOG.A", "BRK.B", "BF-A"]


def test_parse_tickers_handles_finnhub_general_empty_case():
    """The general-news endpoint always emits empty `related`; verify."""
    fixture = _load_fixture()
    for raw in fixture:
        assert _parse_tickers(raw["related"]) == []
