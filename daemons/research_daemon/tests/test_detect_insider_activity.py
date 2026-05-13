"""detect_insider_activity behaviour — filter logic, clusters, first-time, partial fail."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
import requests_mock

from research_daemon import detect_insider_activity as dia_module
from research_daemon import fetch_insider_transactions as fit_module
from research_daemon.config import Config
from research_daemon.detect_insider_activity import detect_insider_activity
from research_daemon.fetch_insider_transactions import FINNHUB_BASE
from research_daemon.http_client import HttpClient


INSIDER_URL = f"{FINNHUB_BASE}/stock/insider-transactions"
_FIXED_TODAY = date(2026, 4, 24)


@pytest.fixture(autouse=True)
def fix_today(monkeypatch):
    # Pin both modules' clocks so recent_start_unix and from/to dates are deterministic.
    monkeypatch.setattr(fit_module, "_today_utc", lambda: _FIXED_TODAY)
    monkeypatch.setattr(dia_module, "_today_utc", lambda: _FIXED_TODAY)


def _raw(
    *,
    name="COOK TIMOTHY D",
    position="Chief Executive Officer",
    code="P",
    change=1_000,
    share=250_000,
    price=175.50,
    currency="USD",
    is_derivative=False,
    transaction_date="2026-04-10",
    filing_date="2026-04-12",
):
    return {
        "name": name, "position": position, "transactionCode": code,
        "change": change, "share": share, "transactionPrice": price,
        "currency": currency, "isDerivative": is_derivative,
        "transactionDate": transaction_date, "filingDate": filing_date,
        "symbol": "AAPL", "source": "Form 4",
    }


def _resp(items):
    return {"symbol": "AAPL", "data": items}


def _ticker_response_map(payloads: dict):
    def matcher(request, context):
        sym = request.qs.get("symbol", [""])[0].upper()
        if sym in payloads:
            return payloads[sym]
        context.status_code = 404
        return {}
    return matcher


# ---------- large-buy filter ----------


def test_large_buy_above_threshold_returned(cfg: Config, client: HttpClient):
    # 1000 shares * $175.50 = $175,500 > $100,000 default
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(code="P", change=1_000, price=175.50)]))
        env = detect_insider_activity(
            ["AAPL"], lookback_days=30, include_first_time_detection=False,
            config=cfg, client=client,
        )
    pt = env["data"]["per_ticker"][0]
    assert len(pt["large_buys"]) == 1
    assert pt["large_buys"][0]["value_usd"] == 175_500.0


def test_small_buy_below_threshold_excluded(cfg: Config, client: HttpClient):
    # 100 shares * $50 = $5,000 < $100,000
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(code="P", change=100, price=50.0)]))
        env = detect_insider_activity(
            ["AAPL"], include_first_time_detection=False, config=cfg, client=client,
        )
    assert env["data"]["per_ticker"][0]["large_buys"] == []


def test_non_purchase_transactions_excluded(cfg: Config, client: HttpClient):
    """Awards/gifts/option exercises/sales must not appear as large_buys."""
    items = [
        _raw(code="A", change=10_000, price=175.0),   # award
        _raw(code="G", change=5_000, price=0),        # gift
        _raw(code="M", change=8_000, price=100.0),    # option exercise
        _raw(code="F", change=2_000, price=175.0),    # tax payment
        _raw(code="S", change=-5_000, price=175.0),   # sale
    ]
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp(items))
        env = detect_insider_activity(
            ["AAPL"], include_first_time_detection=False, config=cfg, client=client,
        )
    assert env["data"]["per_ticker"][0]["large_buys"] == []


def test_min_value_usd_parameter(cfg: Config, client: HttpClient):
    # $80,000 buy — above 50k threshold but below 100k default.
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(code="P", change=1_000, price=80.0)]))
        env_default = detect_insider_activity(
            ["AAPL"], include_first_time_detection=False, config=cfg, client=client,
        )
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(code="P", change=1_000, price=80.0)]))
        env_lowered = detect_insider_activity(
            ["AAPL"], min_value_usd=50_000,
            include_first_time_detection=False, config=cfg, client=client,
        )
    assert env_default["data"]["per_ticker"][0]["large_buys"] == []
    assert len(env_lowered["data"]["per_ticker"][0]["large_buys"]) == 1


def test_null_price_excludes_transaction(cfg: Config, client: HttpClient):
    items = [_raw(code="P", change=1_000)]
    items[0]["transactionPrice"] = None
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp(items))
        env = detect_insider_activity(
            ["AAPL"], include_first_time_detection=False, config=cfg, client=client,
        )
    assert env["data"]["per_ticker"][0]["large_buys"] == []


def test_large_buys_sorted_by_value_desc(cfg: Config, client: HttpClient):
    items = [
        _raw(name="A", code="P", change=1_000, price=500.0),   # $500k
        _raw(name="B", code="P", change=2_000, price=500.0),   # $1M
        _raw(name="C", code="P", change=1_500, price=500.0),   # $750k
    ]
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp(items))
        env = detect_insider_activity(
            ["AAPL"], include_first_time_detection=False, config=cfg, client=client,
        )
    names = [b["insider_name"] for b in env["data"]["per_ticker"][0]["large_buys"]]
    assert names == ["B", "C", "A"]


# ---------- cluster detection ----------


def test_cluster_buy_detected_with_two_distinct_insiders(cfg: Config, client: HttpClient):
    items = [
        _raw(name="A", code="P", change=1_000, price=200.0),
        _raw(name="B", code="P", change=500, price=200.0),
    ]
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp(items))
        env = detect_insider_activity(
            ["AAPL"], include_first_time_detection=False, config=cfg, client=client,
        )
    pt = env["data"]["per_ticker"][0]
    assert pt["cluster_buy_detected"] is True
    assert pt["distinct_buyers"] == 2


def test_cluster_not_detected_with_single_insider(cfg: Config, client: HttpClient):
    items = [
        _raw(name="A", code="P", change=1_000, price=200.0),
        _raw(name="A", code="P", change=500, price=200.0, transaction_date="2026-04-15"),
    ]
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp(items))
        env = detect_insider_activity(
            ["AAPL"], include_first_time_detection=False, config=cfg, client=client,
        )
    pt = env["data"]["per_ticker"][0]
    assert pt["cluster_buy_detected"] is False
    assert pt["distinct_buyers"] == 1


def test_cluster_counts_small_buys_too(cfg: Config, client: HttpClient):
    """Coordination signal doesn't require buys to be above threshold."""
    items = [
        _raw(name="A", code="P", change=10, price=50.0),  # tiny, below threshold
        _raw(name="B", code="P", change=10, price=50.0),  # tiny, below threshold
    ]
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp(items))
        env = detect_insider_activity(
            ["AAPL"], include_first_time_detection=False, config=cfg, client=client,
        )
    pt = env["data"]["per_ticker"][0]
    assert pt["cluster_buy_detected"] is True
    assert pt["distinct_buyers"] == 2
    assert pt["large_buys"] == []  # filtered out by value, but cluster still detected


# ---------- first-time detection ----------


def test_first_time_filer_detected(cfg: Config, client: HttpClient):
    """An insider with purchases in recent window but NO entries in baseline."""
    # Baseline (90 days back): only "KnownInsider" activity.
    baseline_date = (_FIXED_TODAY - timedelta(days=90)).isoformat()
    recent_date = (_FIXED_TODAY - timedelta(days=5)).isoformat()
    items = [
        _raw(name="KnownInsider", code="A", transaction_date=baseline_date,
             filing_date=baseline_date),
        _raw(name="KnownInsider", code="P", change=1_000, price=200.0,
             transaction_date=recent_date, filing_date=recent_date),
        _raw(name="NewInsider", code="P", change=2_000, price=200.0,
             transaction_date=recent_date, filing_date=recent_date),
    ]
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp(items))
        env = detect_insider_activity(
            ["AAPL"], lookback_days=30, first_time_lookback_days=365,
            include_first_time_detection=True, config=cfg, client=client,
        )
    pt = env["data"]["per_ticker"][0]
    by_name = {b["insider_name"]: b for b in pt["large_buys"]}
    assert by_name["KnownInsider"]["is_first_time_filer"] is False
    assert by_name["NewInsider"]["is_first_time_filer"] is True
    assert pt["first_time_buyer_count"] == 1


def test_first_time_detection_off_does_not_flag(cfg: Config, client: HttpClient):
    recent_date = (_FIXED_TODAY - timedelta(days=5)).isoformat()
    items = [
        _raw(name="Someone", code="P", change=1_000, price=200.0,
             transaction_date=recent_date, filing_date=recent_date),
    ]
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp(items))
        env = detect_insider_activity(
            ["AAPL"], include_first_time_detection=False, config=cfg, client=client,
        )
    pt = env["data"]["per_ticker"][0]
    assert pt["large_buys"][0]["is_first_time_filer"] is False
    assert pt["first_time_buyer_count"] == 0
    # Meta field reflects the parameter.
    assert env["data"]["first_time_lookback_days"] is None


def test_first_time_buyer_count_deduplicates(cfg: Config, client: HttpClient):
    """One insider with 3 large buys should count as 1 first-time buyer, not 3."""
    recent_date = (_FIXED_TODAY - timedelta(days=5)).isoformat()
    items = [
        _raw(name="NewInsider", code="P", change=1_000, price=200.0,
             transaction_date=recent_date, filing_date=recent_date),
        _raw(name="NewInsider", code="P", change=800, price=200.0,
             transaction_date=recent_date, filing_date=recent_date),
        _raw(name="NewInsider", code="P", change=1_200, price=200.0,
             transaction_date=recent_date, filing_date=recent_date),
    ]
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp(items))
        env = detect_insider_activity(
            ["AAPL"], lookback_days=30, first_time_lookback_days=365,
            include_first_time_detection=True, config=cfg, client=client,
        )
    pt = env["data"]["per_ticker"][0]
    assert len(pt["large_buys"]) == 3
    assert pt["first_time_buyer_count"] == 1


# ---------- multi-ticker partial failure ----------


def test_multi_ticker_partial_failure(cfg: Config, client: HttpClient):
    aapl_payload = _resp([
        _raw(code="P", change=1_000, price=200.0),
    ])
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_ticker_response_map({"AAPL": aapl_payload}))
        env = detect_insider_activity(
            ["AAPL", "BADTKR"], include_first_time_detection=False,
            config=cfg, client=client,
        )

    assert env["status"] == "ok"
    assert env["data_completeness"] == "partial"
    assert env["data"]["tickers_analyzed"] == 1
    assert env["data"]["tickers_failed"] == 1

    by_ticker = {pt["ticker"]: pt for pt in env["data"]["per_ticker"]}
    assert by_ticker["AAPL"]["error"] is None
    assert by_ticker["BADTKR"]["error"]["reason"] == "not_found"
    # Shape stability — failed ticker still has all fields.
    expected = {"ticker", "large_buys", "cluster_buy_detected",
                "distinct_buyers", "first_time_buyer_count", "error"}
    for pt in env["data"]["per_ticker"]:
        assert set(pt.keys()) == expected

    assert len(env["warnings"]) == 1
    assert env["warnings"][0]["field"] == "per_ticker"
    assert env["warnings"][0]["reason"] == "upstream_error"


def test_ticker_with_no_activity_is_clean_success(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([]))
        env = detect_insider_activity(
            ["AAPL"], include_first_time_detection=False, config=cfg, client=client,
        )
    pt = env["data"]["per_ticker"][0]
    assert pt["error"] is None
    assert pt["large_buys"] == []
    assert pt["cluster_buy_detected"] is False
    assert pt["distinct_buyers"] == 0
    assert env["data_completeness"] == "complete"


# ---------- schema ----------


def test_large_buy_schema_stable(cfg: Config, client: HttpClient):
    expected = {
        "insider_name", "insider_role", "transaction_code",
        "shares", "price_per_share", "value_usd",
        "transacted_at_unix", "transacted_at",
        "filed_at_unix", "filed_at",
        "is_first_time_filer",
    }
    with requests_mock.Mocker() as m:
        m.get(INSIDER_URL, json=_resp([_raw(code="P", change=1_000, price=200.0)]))
        env = detect_insider_activity(
            ["AAPL"], include_first_time_detection=False, config=cfg, client=client,
        )
    buy = env["data"]["per_ticker"][0]["large_buys"][0]
    assert set(buy.keys()) == expected
    assert buy["transacted_at"].endswith("T00:00:00Z")


# ---------- input validation ----------


def test_empty_ticker_list_rejected(cfg: Config, client: HttpClient):
    env = detect_insider_activity([], config=cfg, client=client)
    assert env["status"] == "error"


def test_non_list_tickers_rejected(cfg: Config, client: HttpClient):
    env = detect_insider_activity("AAPL", config=cfg, client=client)  # type: ignore[arg-type]
    assert env["status"] == "error"


def test_too_many_tickers_rejected(cfg: Config, client: HttpClient):
    env = detect_insider_activity(["T"] * 101, config=cfg, client=client)
    assert env["status"] == "error"


def test_whitespace_ticker_rejected(cfg: Config, client: HttpClient):
    env = detect_insider_activity(["AAPL", "   "], config=cfg, client=client)
    assert env["status"] == "error"


@pytest.mark.parametrize("bad", [0, -1, 366])
def test_lookback_days_out_of_range(bad, cfg: Config, client: HttpClient):
    env = detect_insider_activity(
        ["AAPL"], lookback_days=bad, config=cfg, client=client,
    )
    assert env["status"] == "error"


@pytest.mark.parametrize("bad", [0, -1, 1_000_000_001])
def test_min_value_usd_out_of_range(bad, cfg: Config, client: HttpClient):
    env = detect_insider_activity(
        ["AAPL"], min_value_usd=bad, config=cfg, client=client,
    )
    assert env["status"] == "error"


def test_first_time_lookback_not_greater_than_lookback_rejected(
    cfg: Config, client: HttpClient
):
    env = detect_insider_activity(
        ["AAPL"], lookback_days=30, first_time_lookback_days=30,
        include_first_time_detection=True, config=cfg, client=client,
    )
    assert env["status"] == "error"
    assert "first_time" in env["error_detail"]


def test_first_time_lookback_out_of_range_rejected(cfg: Config, client: HttpClient):
    env = detect_insider_activity(
        ["AAPL"], first_time_lookback_days=400, config=cfg, client=client,
    )
    assert env["status"] == "error"


def test_non_bool_include_first_time_rejected(cfg: Config, client: HttpClient):
    env = detect_insider_activity(
        ["AAPL"], include_first_time_detection="yes",  # type: ignore[arg-type]
        config=cfg, client=client,
    )
    assert env["status"] == "error"


@pytest.mark.parametrize("bad", [30.0, "30", None, True])
def test_non_int_lookback_days_rejected(bad, cfg: Config, client: HttpClient):
    env = detect_insider_activity(
        ["AAPL"], lookback_days=bad, config=cfg, client=client,  # type: ignore[arg-type]
    )
    assert env["status"] == "error"
