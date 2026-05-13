"""fetch_news behaviour — per-item parsing, drops, empties, errors, edges."""

from __future__ import annotations

from datetime import date

import pytest
import requests_mock

from research_daemon import fetch_news as fn_module
from research_daemon.config import Config
from research_daemon.fetch_news import FINNHUB_BASE, fetch_news
from research_daemon.http_client import HttpClient


NEWS_URL = f"{FINNHUB_BASE}/company-news"

_FIXED_TODAY = date(2026, 4, 23)


@pytest.fixture(autouse=True)
def fix_today(monkeypatch):
    """Pin today's date so from_date/to_date are deterministic."""
    monkeypatch.setattr(fn_module, "_today_utc", lambda: _FIXED_TODAY)


def _item(
    *,
    id_=1,
    headline="Apple posts record quarter",
    summary="Apple Inc. reported...",
    source="Reuters",
    url="https://example.test/news/1",
    dt=1745000000,
):
    return {
        "category": "company news",
        "datetime": dt,
        "headline": headline,
        "id": id_,
        "image": "https://example.test/img.jpg",
        "related": "AAPL",
        "source": source,
        "summary": summary,
        "url": url,
    }


# ---------- happy path ----------


def test_ok_with_items(cfg: Config, client: HttpClient):
    raw = [_item(id_=100), _item(id_=101, headline="Second headline", dt=1745100000)]
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=raw)
        env = fetch_news("aapl", days=7, config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["source"] == "finnhub"
    assert env["warnings"] == []

    data = env["data"]
    assert data["ticker"] == "AAPL"
    assert data["window_days"] == 7
    assert data["from_date"] == "2026-04-16"
    assert data["to_date"] == "2026-04-23"
    assert data["item_count"] == 2
    assert data["dropped_count"] == 0
    assert len(data["items"]) == 2


def test_default_days_is_seven(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=[])
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["data"]["window_days"] == 7
    assert env["data"]["from_date"] == "2026-04-16"


def test_empty_list_is_complete_with_zero_items(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=[])
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["data"]["item_count"] == 0
    assert env["data"]["items"] == []
    assert env["warnings"] == []


# ---------- per-item schema ----------


def test_per_item_schema_is_stable(cfg: Config, client: HttpClient):
    expected_keys = {
        "id", "headline", "summary", "source", "url",
        "published_at_unix", "published_at",
    }
    raw = [_item(id_=100), _item(id_=200, dt=1745100000, summary="another")]
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=raw)
        env = fetch_news("AAPL", config=cfg, client=client)

    assert len(env["data"]["items"]) == 2
    for item in env["data"]["items"]:
        assert set(item.keys()) == expected_keys
        assert isinstance(item["id"], str)
        assert isinstance(item["headline"], str)
        assert item["summary"] is None or isinstance(item["summary"], str)
        assert isinstance(item["source"], str)
        assert isinstance(item["url"], str)
        assert isinstance(item["published_at_unix"], int)
        assert isinstance(item["published_at"], str)
        assert item["published_at"].endswith("Z")


def test_published_at_derived_from_unix(cfg: Config, client: HttpClient):
    # 1745000000 = 2025-04-18T18:13:20Z
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=[_item(dt=1745000000)])
        env = fetch_news("AAPL", config=cfg, client=client)
    item = env["data"]["items"][0]
    assert item["published_at_unix"] == 1745000000
    assert item["published_at"] == "2025-04-18T18:13:20Z"


def test_empty_summary_normalised_to_none(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=[_item(summary="")])
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["data"]["items"][0]["summary"] is None


def test_whitespace_summary_normalised_to_none(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=[_item(summary="   \n  ")])
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["data"]["items"][0]["summary"] is None


def test_numeric_id_coerced_to_string(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=[_item(id_=987654)])
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["data"]["items"][0]["id"] == "987654"


def test_missing_id_becomes_empty_string(cfg: Config, client: HttpClient):
    raw = [_item()]
    raw[0].pop("id")
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=raw)
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["data"]["items"][0]["id"] == ""


# ---------- drop behaviour ----------


@pytest.mark.parametrize("missing_field", ["headline", "url", "datetime", "source"])
def test_items_missing_required_field_are_dropped(
    missing_field, cfg: Config, client: HttpClient
):
    good = _item(id_=1)
    bad = _item(id_=2)
    bad.pop(missing_field)

    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=[good, bad])
        env = fetch_news("AAPL", config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "partial"
    assert env["data"]["item_count"] == 1
    assert env["data"]["dropped_count"] == 1
    assert env["data"]["items"][0]["id"] == "1"

    assert len(env["warnings"]) == 1
    w = env["warnings"][0]
    assert w["field"] == "items"
    assert w["reason"] == "parse_error"
    assert w["source"] == "finnhub"


def test_non_dict_items_are_dropped(cfg: Config, client: HttpClient):
    raw = [_item(id_=1), "junk string", 42, None]
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=raw)
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["data"]["item_count"] == 1
    assert env["data"]["dropped_count"] == 3
    assert env["data_completeness"] == "partial"


def test_datetime_zero_is_dropped(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=[_item(dt=0)])
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["data"]["item_count"] == 0
    assert env["data"]["dropped_count"] == 1
    assert env["data_completeness"] == "partial"


def test_all_items_dropped_still_returns_ok_partial(cfg: Config, client: HttpClient):
    raw = [_item(headline=""), _item(url="")]
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=raw)
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["status"] == "ok"
    assert env["data_completeness"] == "partial"
    assert env["data"]["item_count"] == 0
    assert env["data"]["dropped_count"] == 2


# ---------- upstream failures ----------


def test_non_list_payload_is_error(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json={"error": "bad request"})
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["status"] == "error"
    assert env["data_completeness"] == "none"
    assert "list" in env["error_detail"]


def test_404_is_not_found(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, status_code=404)
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["status"] == "not_found"
    assert env["data_completeness"] == "none"


def test_429_is_rate_limited(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, status_code=429)
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["status"] == "rate_limited"


def test_500_is_error(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, status_code=500)
        env = fetch_news("AAPL", config=cfg, client=client)
    assert env["status"] == "error"


# ---------- input validation ----------


def test_empty_ticker_rejected(cfg: Config, client: HttpClient):
    env = fetch_news("  ", config=cfg, client=client)
    assert env["status"] == "error"
    assert "non-empty" in env["error_detail"]


def test_non_string_ticker_rejected(cfg: Config, client: HttpClient):
    env = fetch_news(None, config=cfg, client=client)  # type: ignore[arg-type]
    assert env["status"] == "error"


@pytest.mark.parametrize("bad_days", [0, -1, 366, 10_000])
def test_out_of_range_days_rejected(bad_days, cfg: Config, client: HttpClient):
    env = fetch_news("AAPL", days=bad_days, config=cfg, client=client)
    assert env["status"] == "error"
    assert "days" in env["error_detail"]


@pytest.mark.parametrize("bad_days", [7.5, "7", None, True])
def test_non_int_days_rejected(bad_days, cfg: Config, client: HttpClient):
    env = fetch_news("AAPL", days=bad_days, config=cfg, client=client)  # type: ignore[arg-type]
    assert env["status"] == "error"
