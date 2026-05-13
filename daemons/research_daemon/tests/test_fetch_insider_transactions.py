"""fetch_insider_transactions behaviour — parsing, drops, dates, edges."""

from __future__ import annotations

from datetime import date

import pytest
import requests_mock

from research_daemon import fetch_insider_transactions as fit_module
from research_daemon.config import Config
from research_daemon.fetch_insider_transactions import (
    FINNHUB_BASE,
    fetch_insider_transactions,
)
from research_daemon.http_client import HttpClient


INSIDER_URL = f"{FINNHUB_BASE}/stock/insider-transactions"

_FIXED_TODAY = date(2026, 4, 23)


@pytest.fixture(autouse=True)
def fix_today(monkeypatch):
    monkeypatch.setattr(fit_module, "_today_utc", lambda: _FIXED_TODAY)


def _raw(
    *,
    name="COOK TIMOTHY D",
    position="Chief Executive Officer",
    code="S",
    change=-1000,
    share=250_000,
    price=175.50,
    currency="USD",
    is_derivative=False,
    transaction_date="2026-04-10",
    filing_date="2026-04-12",
):
    return {
        "name": name,
        "position": position,
        "transactionCode": code,
        "change": change,
        "share": share,
        "transactionPrice": price,
        "currency": currency,
        "isDerivative": is_derivative,
        "transactionDate": transaction_date,
        "filingDate": filing_date,
        "symbol": "AAPL",
        "source": "Form 4",
    }


def _resp(items):
    return {"symbol": "AAPL", "data": items}


# ---------- happy path ----------


def test_ok_with_items(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(), _raw(code="P", change=500, price=170.0)]))
        env = fetch_insider_transactions("aapl", days=30, config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["warnings"] == []

    data = env["data"]
    assert data["ticker"] == "AAPL"
    assert data["window_days"] == 30
    assert data["from_date"] == "2026-03-24"
    assert data["to_date"] == "2026-04-23"
    assert data["item_count"] == 2
    assert data["dropped_count"] == 0


def test_default_days_is_thirty(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["window_days"] == 30
    assert env["data"]["from_date"] == "2026-03-24"


def test_empty_list_complete(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["data"]["item_count"] == 0
    assert env["data"]["items"] == []


# ---------- per-item schema ----------


def test_per_item_schema_is_stable(cfg: Config, client: HttpClient):
    expected_keys = {
        "insider_name", "insider_role", "transaction_code", "transaction_type",
        "shares", "shares_held_after", "price_per_share", "currency", "is_derivative",
        "transacted_at_unix", "transacted_at", "filed_at_unix", "filed_at",
    }
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(), _raw(code="P", change=500)]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)

    for item in env["data"]["items"]:
        assert set(item.keys()) == expected_keys
        assert isinstance(item["insider_name"], str)
        assert item["insider_role"] is None or isinstance(item["insider_role"], str)
        assert isinstance(item["transaction_code"], str)
        assert isinstance(item["transaction_type"], str)
        assert isinstance(item["shares"], int)
        assert item["shares_held_after"] is None or isinstance(item["shares_held_after"], int)
        assert item["price_per_share"] is None or isinstance(item["price_per_share"], float)
        assert item["currency"] is None or isinstance(item["currency"], str)
        assert isinstance(item["is_derivative"], bool)
        assert isinstance(item["transacted_at_unix"], int)
        assert isinstance(item["transacted_at"], str)
        assert item["transacted_at"].endswith("T00:00:00Z")
        assert isinstance(item["filed_at_unix"], int)
        assert isinstance(item["filed_at"], str)
        assert item["filed_at"].endswith("T00:00:00Z")


def test_signed_shares_preserved(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([
            _raw(code="S", change=-1000),
            _raw(code="P", change=500),
        ]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    items = env["data"]["items"]
    assert items[0]["shares"] == -1000
    assert items[1]["shares"] == 500


@pytest.mark.parametrize("code,expected_type", [
    ("S", "sale"),
    ("P", "purchase"),
    ("A", "award"),
    ("G", "gift"),
    ("D", "disposition"),
    ("F", "tax_payment"),
    ("M", "option_exercise"),
    ("X", "option_exercise"),
    ("C", "conversion"),
])
def test_transaction_type_mapping(code, expected_type, cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(code=code)]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    item = env["data"]["items"][0]
    assert item["transaction_code"] == code
    assert item["transaction_type"] == expected_type


@pytest.mark.parametrize("code", ["J", "K", "W", "Z", "Q"])
def test_unmapped_codes_fall_through_to_other(code, cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(code=code)]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    item = env["data"]["items"][0]
    assert item["transaction_code"] == code
    assert item["transaction_type"] == "other"


def test_zero_price_preserved_not_null(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(code="A", change=5000, price=0)]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["items"][0]["price_per_share"] == 0.0


def test_missing_price_becomes_null(cfg: Config, client: HttpClient):
    raw = _raw()
    raw.pop("transactionPrice")
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([raw]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["items"][0]["price_per_share"] is None


def test_missing_position_becomes_null(cfg: Config, client: HttpClient):
    raw = _raw()
    raw.pop("position")
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([raw]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["items"][0]["insider_role"] is None


def test_missing_share_becomes_null_held_after(cfg: Config, client: HttpClient):
    raw = _raw()
    raw.pop("share")
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([raw]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["items"][0]["shares_held_after"] is None


def test_missing_currency_becomes_null(cfg: Config, client: HttpClient):
    raw = _raw()
    raw.pop("currency")
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([raw]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["items"][0]["currency"] is None


def test_missing_is_derivative_defaults_false(cfg: Config, client: HttpClient):
    raw = _raw()
    raw.pop("isDerivative")
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([raw]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["items"][0]["is_derivative"] is False


# ---------- date handling ----------


def test_dates_parsed_to_midnight_utc(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(
            transaction_date="2026-04-10",
            filing_date="2026-04-12",
        )]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    item = env["data"]["items"][0]
    assert item["transacted_at"] == "2026-04-10T00:00:00Z"
    assert item["filed_at"] == "2026-04-12T00:00:00Z"
    # 2026-04-10 00:00:00 UTC epoch = 1775779200
    assert item["transacted_at_unix"] == 1775779200
    # 2026-04-12 00:00:00 UTC epoch = 1775952000
    assert item["filed_at_unix"] == 1775952000
    # transacted_at should be earlier than or equal to filed_at in normal data.
    assert item["transacted_at_unix"] <= item["filed_at_unix"]


def test_longer_iso_timestamp_accepted(cfg: Config, client: HttpClient):
    """Finnhub occasionally returns 'YYYY-MM-DD HH:MM:SS' — take date portion."""
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(
            transaction_date="2026-04-10 14:30:00",
            filing_date="2026-04-12T09:00:00Z",
        )]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["item_count"] == 1
    item = env["data"]["items"][0]
    assert item["transacted_at"] == "2026-04-10T00:00:00Z"
    assert item["filed_at"] == "2026-04-12T00:00:00Z"


# ---------- drop behaviour ----------


@pytest.mark.parametrize("field", [
    "name", "transactionCode", "change", "transactionDate", "filingDate",
])
def test_items_missing_required_field_are_dropped(field, cfg: Config, client: HttpClient):
    good = _raw()
    bad = _raw()
    bad.pop(field)
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([good, bad]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "partial"
    assert env["data"]["item_count"] == 1
    assert env["data"]["dropped_count"] == 1
    assert len(env["warnings"]) == 1
    assert env["warnings"][0]["field"] == "items"
    assert env["warnings"][0]["reason"] == "parse_error"


def test_non_dict_items_dropped(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(), "junk", 0, None]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["item_count"] == 1
    assert env["data"]["dropped_count"] == 3
    assert env["data_completeness"] == "partial"


def test_invalid_date_strings_cause_drop(cfg: Config, client: HttpClient):
    raw = _raw(transaction_date="not-a-date")
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([raw]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["item_count"] == 0
    assert env["data"]["dropped_count"] == 1


def test_empty_insider_name_dropped(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(name="   ")]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["item_count"] == 0


def test_bool_change_rejected(cfg: Config, client: HttpClient):
    """A boolean `change` value slipped in from bad upstream — drop it."""
    raw = _raw()
    raw["change"] = True
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([raw]))
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["data"]["item_count"] == 0


# ---------- upstream failures ----------


def test_wrong_payload_shape_is_error(cfg: Config, client: HttpClient):
    # Finnhub responded with a list instead of {data: [...]}.
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=[_raw()])
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["status"] == "error"
    assert env["data_completeness"] == "none"


def test_missing_data_key_is_error(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json={"symbol": "AAPL"})
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["status"] == "error"


def test_404_is_not_found(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, status_code=404)
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["status"] == "not_found"


def test_429_is_rate_limited(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, status_code=429)
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["status"] == "rate_limited"


def test_500_is_error(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, status_code=500)
        env = fetch_insider_transactions("AAPL", config=cfg, client=client)
    assert env["status"] == "error"


# ---------- input validation ----------


def test_empty_ticker_rejected(cfg: Config, client: HttpClient):
    env = fetch_insider_transactions("  ", config=cfg, client=client)
    assert env["status"] == "error"


def test_non_string_ticker_rejected(cfg: Config, client: HttpClient):
    env = fetch_insider_transactions(None, config=cfg, client=client)  # type: ignore[arg-type]
    assert env["status"] == "error"


@pytest.mark.parametrize("bad_days", [0, -1, 366, 10_000])
def test_out_of_range_days_rejected(bad_days, cfg: Config, client: HttpClient):
    env = fetch_insider_transactions("AAPL", days=bad_days, config=cfg, client=client)
    assert env["status"] == "error"
    assert "days" in env["error_detail"]


@pytest.mark.parametrize("bad_days", [7.5, "7", None, True])
def test_non_int_days_rejected(bad_days, cfg: Config, client: HttpClient):
    env = fetch_insider_transactions("AAPL", days=bad_days, config=cfg, client=client)  # type: ignore[arg-type]
    assert env["status"] == "error"
