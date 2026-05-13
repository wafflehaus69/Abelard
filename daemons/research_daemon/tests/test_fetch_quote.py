"""fetch_quote behaviour — full stack, mocked at the HTTP layer."""

from __future__ import annotations

import requests_mock

from research_daemon.config import Config
from research_daemon.fetch_quote import FINNHUB_BASE, fetch_quote
from research_daemon.http_client import HttpClient


QUOTE_URL = f"{FINNHUB_BASE}/quote"
METRIC_URL = f"{FINNHUB_BASE}/stock/metric"


_QUOTE_OK = {
    "c": 261.74, "d": 0.66, "dp": 0.2527,
    "o": 261.07, "h": 263.31, "l": 260.68,
    "pc": 261.08, "t": 1735920000,
}
_METRIC_OK = {"metric": {"52WeekHigh": 270.1, "52WeekLow": 164.08}}


def _warnings_by_field(env: dict) -> dict[str, dict]:
    return {w["field"]: w for w in env["warnings"]}


def test_ok_envelope_shape(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, json=_QUOTE_OK)
        m.get(METRIC_URL, json=_METRIC_OK)
        env = fetch_quote("aapl", config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["source"] == "finnhub"
    assert env["error_detail"] is None
    assert env["timestamp"].endswith("Z")

    data = env["data"]
    assert data["ticker"] == "AAPL"
    assert data["price"] == 261.74
    assert data["day_high"] == 263.31
    assert data["day_low"] == 260.68
    assert data["previous_close"] == 261.08
    assert data["quote_time_unix"] == 1735920000
    assert data["week_52_high"] == 270.1
    assert data["week_52_low"] == 164.08
    assert data["volume"] is None
    # No warnings field inside data — warnings live at envelope level.
    assert "warnings" not in data


def test_volume_warning_is_structured(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, json=_QUOTE_OK)
        m.get(METRIC_URL, json=_METRIC_OK)
        env = fetch_quote("AAPL", config=cfg, client=client)

    by_field = _warnings_by_field(env)
    assert "volume" in by_field
    vol = by_field["volume"]
    assert vol["reason"] == "not_available_on_free_tier"
    assert vol["source"] == "finnhub"
    assert "suggestion" in vol


def test_ticker_is_uppercased_and_stripped(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, json=_QUOTE_OK)
        m.get(METRIC_URL, json=_METRIC_OK)
        env = fetch_quote("  msft  ", config=cfg, client=client)
    assert env["data"]["ticker"] == "MSFT"


def test_secondary_500_returns_ok_partial_with_structured_warnings(
    cfg: Config, client: HttpClient
):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, json=_QUOTE_OK)
        m.get(METRIC_URL, status_code=500)
        env = fetch_quote("AAPL", config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "partial"
    assert env["data"]["week_52_high"] is None
    assert env["data"]["week_52_low"] is None

    by_field = _warnings_by_field(env)
    assert by_field["week_52_high"]["reason"] == "upstream_error"
    assert by_field["week_52_low"]["reason"] == "upstream_error"
    assert by_field["week_52_high"]["source"] == "finnhub"


def test_secondary_429_reports_rate_limited_reason(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, json=_QUOTE_OK)
        m.get(METRIC_URL, status_code=429)
        env = fetch_quote("AAPL", config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "partial"
    by_field = _warnings_by_field(env)
    assert by_field["week_52_high"]["reason"] == "rate_limited"
    assert by_field["week_52_low"]["reason"] == "rate_limited"


def test_secondary_missing_field_reports_missing_field_reason(
    cfg: Config, client: HttpClient
):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, json=_QUOTE_OK)
        m.get(METRIC_URL, json={"metric": {"52WeekHigh": 270.1}})  # no 52WeekLow
        env = fetch_quote("AAPL", config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "partial"
    by_field = _warnings_by_field(env)
    assert "week_52_low" in by_field
    assert by_field["week_52_low"]["reason"] == "missing_field"
    # The field that was populated should NOT have a degradation warning.
    assert "week_52_high" not in by_field


def test_not_found_for_all_zero_payload(cfg: Config, client: HttpClient):
    zeros = {k: 0 for k in ("c", "d", "dp", "o", "h", "l", "pc", "t")}
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, json=zeros)
        env = fetch_quote("NOPE", config=cfg, client=client)
    assert env["status"] == "not_found"
    assert env["data_completeness"] == "none"
    assert env["data"] is None
    assert env["warnings"] == []


def test_primary_404_is_not_found(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, status_code=404)
        env = fetch_quote("AAPL", config=cfg, client=client)
    assert env["status"] == "not_found"
    assert env["data_completeness"] == "none"


def test_primary_429_is_rate_limited(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, status_code=429)
        env = fetch_quote("AAPL", config=cfg, client=client)
    assert env["status"] == "rate_limited"
    assert env["data_completeness"] == "none"


def test_primary_500_is_error(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, status_code=500)
        env = fetch_quote("AAPL", config=cfg, client=client)
    assert env["status"] == "error"
    assert env["data_completeness"] == "none"
    assert env["data"] is None
    assert "500" in env["error_detail"]


def test_empty_ticker_is_validation_error(cfg: Config, client: HttpClient):
    env = fetch_quote("   ", config=cfg, client=client)
    assert env["status"] == "error"
    assert env["data_completeness"] == "none"
    assert "non-empty" in env["error_detail"]


def test_non_string_ticker_is_validation_error(cfg: Config, client: HttpClient):
    env = fetch_quote(None, config=cfg, client=client)  # type: ignore[arg-type]
    assert env["status"] == "error"


def test_api_key_not_in_warnings_or_error_detail(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, status_code=500)
        env = fetch_quote("AAPL", config=cfg, client=client)
    # error_detail contains the URL with query string; key must be scrubbed.
    assert cfg.finnhub_api_key not in (env["error_detail"] or "")
    for w in env["warnings"]:
        assert cfg.finnhub_api_key not in str(w)
