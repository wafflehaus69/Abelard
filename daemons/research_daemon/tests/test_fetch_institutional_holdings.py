"""fetch_institutional_holdings behaviour — yfinance-backed.

Tests mock yfinance.Ticker to return list-of-dicts as the "DataFrame" —
the parse code accepts either pandas DataFrames or lists of dicts, so tests
stay hermetic without importing pandas explicitly.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from research_daemon import fetch_institutional_holdings as fih_module
from research_daemon.fetch_institutional_holdings import fetch_institutional_holdings


def _row(
    *,
    holder="Vanguard Group Inc",
    date_reported="2026-03-31",
    shares=1_000_000,
    pct_held=0.05,
    pct_change=0.02,
    value=200_000_000,
):
    return {
        "Date Reported": datetime.fromisoformat(date_reported) if date_reported else None,
        "Holder": holder,
        "pctHeld": pct_held,
        "Shares": shares,
        "Value": value,
        "pctChange": pct_change,
    }


@pytest.fixture
def mock_ticker(monkeypatch):
    """Patch yf.Ticker to return a MagicMock. Test sets .institutional_holders
    and .mutualfund_holders on the returned mock."""
    mock = MagicMock()
    instance = MagicMock()
    mock.return_value = instance
    monkeypatch.setattr(fih_module.yf, "Ticker", mock)
    return instance


# ---------- happy path ----------


def test_ok_with_institutional_and_mutualfund_merged(mock_ticker):
    mock_ticker.institutional_holders = [
        _row(holder="BlackRock Inc", shares=2_800_000, pct_change=-0.01),
    ]
    mock_ticker.mutualfund_holders = [
        _row(holder="Vanguard 500 Index Fund", shares=1_000_000, pct_change=0.02),
    ]
    env = fetch_institutional_holdings("AAPL")

    assert env["status"] == "ok"
    assert env["source"] == "yahoo"
    assert env["data_completeness"] == "complete"
    assert env["warnings"] == []

    data = env["data"]
    assert data["ticker"] == "AAPL"
    assert data["as_of_quarter"] == "2026Q1"
    assert data["holders_returned"] == 2
    assert data["holders_total_in_quarter"] == 2
    assert data["dropped_count"] == 0

    # Sorted by shares desc.
    names = [h["name"] for h in data["holders"]]
    assert names == ["BlackRock Inc", "Vanguard 500 Index Fund"]

    # holder_type tagged correctly.
    types = {h["name"]: h["holder_type"] for h in data["holders"]}
    assert types["BlackRock Inc"] == "institution"
    assert types["Vanguard 500 Index Fund"] == "mutual_fund"


def test_top_n_applied_after_merge(mock_ticker):
    mock_ticker.institutional_holders = [
        _row(holder=f"Inst-{i}", shares=(10 - i) * 1_000_000)
        for i in range(5)
    ]
    mock_ticker.mutualfund_holders = [
        _row(holder=f"MF-{i}", shares=(5 - i) * 500_000)
        for i in range(5)
    ]
    env = fetch_institutional_holdings("AAPL", top_n=3)
    data = env["data"]
    assert data["holders_returned"] == 3
    assert data["holders_total_in_quarter"] == 10
    # Top 3 by shares are the 3 largest institutions.
    names = [h["name"] for h in data["holders"]]
    assert names == ["Inst-0", "Inst-1", "Inst-2"]


def test_empty_holders_still_complete(mock_ticker):
    mock_ticker.institutional_holders = None
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("ARRN")
    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["data"]["holders_returned"] == 0
    assert env["data"]["holders"] == []
    assert env["data"]["as_of_quarter"] is None
    assert env["data"]["reported_at"] is None


def test_only_institutional_present(mock_ticker):
    mock_ticker.institutional_holders = [_row(holder="Only Inst", shares=500_000)]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    assert env["data"]["holders_returned"] == 1
    assert env["data"]["holders"][0]["holder_type"] == "institution"


def test_only_mutualfund_present(mock_ticker):
    mock_ticker.institutional_holders = None
    mock_ticker.mutualfund_holders = [_row(holder="Only MF", shares=800_000)]
    env = fetch_institutional_holdings("AAPL")
    assert env["data"]["holders_returned"] == 1
    assert env["data"]["holders"][0]["holder_type"] == "mutual_fund"


# ---------- per-holder schema ----------


def test_per_holder_schema_stable(mock_ticker):
    expected = {
        "name", "cik", "holder_type", "shares",
        "shares_change_qoq", "qoq_pct_change",
        "portfolio_percent", "percent_of_shares_held",
        "filed_at_unix", "filed_at",
    }
    mock_ticker.institutional_holders = [_row()]
    mock_ticker.mutualfund_holders = [_row(holder="Fund X")]
    env = fetch_institutional_holdings("AAPL")
    for h in env["data"]["holders"]:
        assert set(h.keys()) == expected


def test_cik_and_portfolio_percent_always_null(mock_ticker):
    """Semantic mismatches from Yahoo: cik unavailable, portfolio_percent
    would mean something different so we don't populate it."""
    mock_ticker.institutional_holders = [_row()]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    h = env["data"]["holders"][0]
    assert h["cik"] is None
    assert h["portfolio_percent"] is None
    assert h["percent_of_shares_held"] == 0.05


def test_qoq_pct_change_and_derived_shares_change(mock_ticker):
    """pctChange=0.02 on 1_020_000 shares → prior ≈ 1_000_000, delta ≈ +20_000."""
    mock_ticker.institutional_holders = [
        _row(shares=1_020_000, pct_change=0.02),
    ]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    h = env["data"]["holders"][0]
    assert h["qoq_pct_change"] == 0.02
    assert h["shares_change_qoq"] == 20_000


def test_qoq_pct_change_negative(mock_ticker):
    """pctChange=-0.01 on 990_000 shares → prior=1_000_000, delta=-10_000."""
    mock_ticker.institutional_holders = [
        _row(shares=990_000, pct_change=-0.01),
    ]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    h = env["data"]["holders"][0]
    assert h["qoq_pct_change"] == -0.01
    assert h["shares_change_qoq"] == -10_000


def test_missing_pct_change_gives_null_delta(mock_ticker):
    row = _row()
    row.pop("pctChange")
    mock_ticker.institutional_holders = [row]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    h = env["data"]["holders"][0]
    assert h["qoq_pct_change"] is None
    assert h["shares_change_qoq"] is None


def test_pct_change_minus_one_avoids_divide_by_zero(mock_ticker):
    """Defensive: (1 + pctChange) = 0 would break the derivation."""
    mock_ticker.institutional_holders = [_row(pct_change=-1.0)]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    h = env["data"]["holders"][0]
    assert h["qoq_pct_change"] == -1.0
    assert h["shares_change_qoq"] is None


def test_missing_pct_held_gives_null(mock_ticker):
    row = _row()
    row.pop("pctHeld")
    mock_ticker.institutional_holders = [row]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    assert env["data"]["holders"][0]["percent_of_shares_held"] is None


def test_filed_at_derived_from_date_reported(mock_ticker):
    mock_ticker.institutional_holders = [_row(date_reported="2026-03-31")]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    h = env["data"]["holders"][0]
    assert h["filed_at"] == "2026-03-31T00:00:00Z"
    assert h["filed_at_unix"] == int(datetime(2026, 3, 31, tzinfo=timezone.utc).timestamp())


def test_string_date_accepted(mock_ticker):
    row = _row()
    row["Date Reported"] = "2026-03-31"
    mock_ticker.institutional_holders = [row]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    assert env["data"]["holders"][0]["filed_at"] == "2026-03-31T00:00:00Z"


# ---------- drops ----------


@pytest.mark.parametrize("bad_field", ["Holder", "Shares", "Date Reported"])
def test_missing_required_field_drops_row(bad_field, mock_ticker):
    good = _row()
    bad = _row()
    bad.pop(bad_field)
    mock_ticker.institutional_holders = [good, bad]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    assert env["data"]["holders_returned"] == 1
    assert env["data"]["dropped_count"] == 1
    assert env["data_completeness"] == "partial"
    assert any(w["reason"] == "parse_error" for w in env["warnings"])


def test_empty_holder_name_dropped(mock_ticker):
    mock_ticker.institutional_holders = [_row(holder="   ")]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    assert env["data"]["holders_returned"] == 0
    assert env["data"]["dropped_count"] == 1


def test_zero_or_negative_shares_dropped(mock_ticker):
    mock_ticker.institutional_holders = [
        _row(holder="Zero", shares=0),
        _row(holder="Negative", shares=-100),
    ]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    assert env["data"]["holders_returned"] == 0
    assert env["data"]["dropped_count"] == 2


def test_bool_shares_dropped(mock_ticker):
    row = _row()
    row["Shares"] = True
    mock_ticker.institutional_holders = [row]
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("AAPL")
    assert env["data"]["dropped_count"] == 1


# ---------- yfinance failure ----------


def test_yfinance_exception_becomes_error(monkeypatch):
    def boom(_sym):
        raise ConnectionError("Yahoo blocked us")
    monkeypatch.setattr(fih_module.yf, "Ticker", boom)
    env = fetch_institutional_holdings("AAPL")
    assert env["status"] == "error"
    assert env["source"] == "yahoo"
    assert env["data_completeness"] == "none"
    assert "Yahoo blocked us" in env["error_detail"]


# ---------- input validation ----------


def test_empty_ticker_rejected():
    env = fetch_institutional_holdings("  ")
    assert env["status"] == "error"
    assert "non-empty" in env["error_detail"]


def test_non_string_ticker_rejected():
    env = fetch_institutional_holdings(None)  # type: ignore[arg-type]
    assert env["status"] == "error"


@pytest.mark.parametrize("bad", [0, -1, 101])
def test_top_n_out_of_range(bad):
    env = fetch_institutional_holdings("AAPL", top_n=bad)
    assert env["status"] == "error"
    assert "top_n" in env["error_detail"]


@pytest.mark.parametrize("bad", [1.5, "5", None, True])
def test_top_n_non_int_rejected(bad):
    env = fetch_institutional_holdings("AAPL", top_n=bad)  # type: ignore[arg-type]
    assert env["status"] == "error"


def test_num_quarters_two_or_more_rejected():
    """yfinance only exposes a single snapshot; multi-quarter is unsupported."""
    env = fetch_institutional_holdings("AAPL", num_quarters=2)
    assert env["status"] == "error"
    assert "num_quarters" in env["error_detail"]
    assert "snapshot" in env["error_detail"].lower()


def test_num_quarters_zero_rejected():
    env = fetch_institutional_holdings("AAPL", num_quarters=0)
    assert env["status"] == "error"


@pytest.mark.parametrize("bad", [1.5, "1", None, True])
def test_num_quarters_non_int_rejected(bad):
    env = fetch_institutional_holdings("AAPL", num_quarters=bad)  # type: ignore[arg-type]
    assert env["status"] == "error"


def test_ticker_upper_cased(mock_ticker):
    mock_ticker.institutional_holders = None
    mock_ticker.mutualfund_holders = None
    env = fetch_institutional_holdings("  aapl  ")
    assert env["data"]["ticker"] == "AAPL"
