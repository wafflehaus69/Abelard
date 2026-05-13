"""detect_institutional_changes behaviour — set-diff, thresholds, partial failures."""

from __future__ import annotations

from datetime import date

import pytest
import requests_mock

from research_daemon import fetch_institutional_holdings as fih_module
from research_daemon.config import Config
from research_daemon.detect_institutional_changes import detect_institutional_changes
from research_daemon.fetch_institutional_holdings import FINNHUB_BASE
from research_daemon.http_client import HttpClient


OWNERSHIP_URL = f"{FINNHUB_BASE}/institutional/ownership"
_FIXED_TODAY = date(2026, 4, 24)


@pytest.fixture(autouse=True)
def fix_today(monkeypatch):
    monkeypatch.setattr(fih_module, "_today_utc", lambda: _FIXED_TODAY)


def _holder(
    *,
    name="Vanguard Group Inc",
    cik="0000102909",
    share=1_000_000,
    change=0,
    portfolio_percent=1.0,
    report_date="2025-12-31",
    filing_date="2026-02-14",
):
    return {
        "name": name, "cik": cik, "share": share, "change": change,
        "filingDate": filing_date, "reportDate": report_date,
        "portfolioPercent": portfolio_percent,
    }


def _resp(holders, symbol="AAPL"):
    return {"symbol": symbol, "cusip": "", "ownership": holders}


# Matcher helper that routes all ownership calls to one payload keyed by
# the `symbol` query param — simulates multi-ticker behaviour.
def _ticker_response_map(payloads: dict[str, dict]):
    def matcher(request, context):
        sym = request.qs.get("symbol", [""])[0].upper()
        if sym in payloads:
            return payloads[sym]
        context.status_code = 404
        return {}
    return matcher


# ---------- happy path ----------


def test_single_ticker_with_mix_of_change_types(cfg: Config, client: HttpClient):
    # Q4 holders: Vanguard (up 20%), BlackRock (flat), Fidelity (NEW), no State Street (CLOSED).
    # Q3 holders: Vanguard (smaller), BlackRock (same), State Street (will close).
    q4 = [
        _holder(name="Vanguard", cik="V", share=1_200_000, report_date="2025-12-31", filing_date="2026-02-10"),
        _holder(name="BlackRock", cik="B", share=1_000_000, report_date="2025-12-31", filing_date="2026-02-10"),
        _holder(name="Fidelity", cik="F", share=500_000, report_date="2025-12-31", filing_date="2026-02-12"),
    ]
    q3 = [
        _holder(name="Vanguard", cik="V", share=1_000_000, report_date="2025-09-30", filing_date="2025-11-10"),
        _holder(name="BlackRock", cik="B", share=1_000_000, report_date="2025-09-30", filing_date="2025-11-10"),
        _holder(name="State Street", cik="S", share=800_000, report_date="2025-09-30", filing_date="2025-11-10"),
    ]

    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(q4 + q3))
        env = detect_institutional_changes(["AAPL"], min_change_pct=10, config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["warnings"] == []

    data = env["data"]
    assert data["ticker_count"] == 1
    assert data["tickers_analyzed"] == 1
    assert data["tickers_failed"] == 0

    pt = data["per_ticker"][0]
    assert pt["ticker"] == "AAPL"
    assert pt["current_quarter"] == "2025Q4"
    assert pt["prior_quarter"] == "2025Q3"
    assert pt["error"] is None

    # New position: Fidelity
    assert len(pt["new_positions"]) == 1
    assert pt["new_positions"][0]["name"] == "Fidelity"

    # Closed: State Street
    assert len(pt["closed_positions"]) == 1
    assert pt["closed_positions"][0]["name"] == "State Street"
    assert pt["closed_positions"][0]["prior_shares"] == 800_000

    # Increased: Vanguard (+20%)
    assert len(pt["increased_positions"]) == 1
    assert pt["increased_positions"][0]["name"] == "Vanguard"
    assert pt["increased_positions"][0]["change_pct"] == 20.0
    assert pt["increased_positions"][0]["prior_shares"] == 1_000_000
    assert pt["increased_positions"][0]["current_shares"] == 1_200_000

    # BlackRock unchanged → not in any bucket.
    assert all("BlackRock" not in str(e) for e in pt["increased_positions"] + pt["reduced_positions"])
    assert len(pt["reduced_positions"]) == 0


def test_min_change_pct_filter(cfg: Config, client: HttpClient):
    # Vanguard: +11% — just above default 10. Passes at 10, fails at 15.
    q4 = [
        _holder(name="Vanguard", cik="V", share=1_110_000, report_date="2025-12-31", filing_date="2026-02-10"),
    ]
    q3 = [
        _holder(name="Vanguard", cik="V", share=1_000_000, report_date="2025-09-30", filing_date="2025-11-10"),
    ]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(q4 + q3))
        env10 = detect_institutional_changes(["AAPL"], min_change_pct=10, config=cfg, client=client)
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(q4 + q3))
        env15 = detect_institutional_changes(["AAPL"], min_change_pct=15, config=cfg, client=client)

    assert len(env10["data"]["per_ticker"][0]["increased_positions"]) == 1
    assert len(env15["data"]["per_ticker"][0]["increased_positions"]) == 0


def test_reduced_position_captured(cfg: Config, client: HttpClient):
    q4 = [_holder(name="Seller", cik="S", share=600_000, report_date="2025-12-31", filing_date="2026-02-10")]
    q3 = [_holder(name="Seller", cik="S", share=1_000_000, report_date="2025-09-30", filing_date="2025-11-10")]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(q4 + q3))
        env = detect_institutional_changes(["AAPL"], min_change_pct=10, config=cfg, client=client)
    pt = env["data"]["per_ticker"][0]
    assert len(pt["reduced_positions"]) == 1
    assert pt["reduced_positions"][0]["change_pct"] == -40.0


def test_no_changes_above_threshold_returns_empty_buckets_without_error(
    cfg: Config, client: HttpClient
):
    q4 = [_holder(name="Steady", cik="S", share=1_050_000, report_date="2025-12-31", filing_date="2026-02-10")]
    q3 = [_holder(name="Steady", cik="S", share=1_000_000, report_date="2025-09-30", filing_date="2025-11-10")]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(q4 + q3))
        env = detect_institutional_changes(["AAPL"], min_change_pct=10, config=cfg, client=client)
    pt = env["data"]["per_ticker"][0]
    assert pt["error"] is None
    assert pt["new_positions"] == []
    assert pt["closed_positions"] == []
    assert pt["increased_positions"] == []
    assert pt["reduced_positions"] == []
    assert env["data_completeness"] == "complete"


# ---------- multi-ticker ----------


def test_multi_ticker_all_succeed(cfg: Config, client: HttpClient):
    aapl_payload = _resp([
        _holder(name="V", cik="V", share=1_200_000, report_date="2025-12-31", filing_date="2026-02-10"),
        _holder(name="V", cik="V", share=1_000_000, report_date="2025-09-30", filing_date="2025-11-10"),
    ])
    msft_payload = _resp([
        _holder(name="B", cik="B", share=2_400_000, report_date="2025-12-31", filing_date="2026-02-10"),
        _holder(name="B", cik="B", share=2_000_000, report_date="2025-09-30", filing_date="2025-11-10"),
    ])
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_ticker_response_map({"AAPL": aapl_payload, "MSFT": msft_payload}))
        env = detect_institutional_changes(["AAPL", "MSFT"], min_change_pct=10, config=cfg, client=client)

    assert env["data"]["tickers_analyzed"] == 2
    assert env["data"]["tickers_failed"] == 0
    tickers = {pt["ticker"] for pt in env["data"]["per_ticker"]}
    assert tickers == {"AAPL", "MSFT"}
    for pt in env["data"]["per_ticker"]:
        assert pt["error"] is None
        assert len(pt["increased_positions"]) == 1


def test_multi_ticker_partial_failure(cfg: Config, client: HttpClient):
    aapl_payload = _resp([
        _holder(name="V", cik="V", share=1_200_000, report_date="2025-12-31", filing_date="2026-02-10"),
        _holder(name="V", cik="V", share=1_000_000, report_date="2025-09-30", filing_date="2025-11-10"),
    ])
    # BADTKR missing from map → 404

    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_ticker_response_map({"AAPL": aapl_payload}))
        env = detect_institutional_changes(
            ["AAPL", "BADTKR"], min_change_pct=10, config=cfg, client=client,
        )

    assert env["status"] == "ok"
    assert env["data_completeness"] == "partial"
    assert env["data"]["tickers_analyzed"] == 1
    assert env["data"]["tickers_failed"] == 1

    by_ticker = {pt["ticker"]: pt for pt in env["data"]["per_ticker"]}
    assert by_ticker["AAPL"]["error"] is None
    assert by_ticker["BADTKR"]["error"] is not None
    assert by_ticker["BADTKR"]["error"]["reason"] == "not_found"
    # Failed ticker still has the shell shape.
    assert by_ticker["BADTKR"]["new_positions"] == []
    assert by_ticker["BADTKR"]["current_quarter"] is None

    # Envelope aggregate warning.
    assert len(env["warnings"]) == 1
    w = env["warnings"][0]
    assert w["field"] == "per_ticker"
    assert w["reason"] == "upstream_error"


def test_ticker_with_only_one_quarter_is_insufficient_history(cfg: Config, client: HttpClient):
    # Only Q4 data present, no Q3 holders.
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([
            _holder(report_date="2025-12-31", filing_date="2026-02-10"),
        ]))
        env = detect_institutional_changes(["AAPL"], min_change_pct=10, config=cfg, client=client)

    assert env["data_completeness"] == "partial"
    pt = env["data"]["per_ticker"][0]
    assert pt["error"]["reason"] == "insufficient_history"
    assert pt["current_quarter"] == "2025Q4"  # still surfaced when known
    assert pt["prior_quarter"] is None
    assert pt["new_positions"] == []


def test_ticker_with_zero_holdings_is_insufficient_history(cfg: Config, client: HttpClient):
    """Small-cap: empty ownership array → insufficient_history, not clean success."""
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([]))
        env = detect_institutional_changes(["ARRN"], min_change_pct=10, config=cfg, client=client)
    pt = env["data"]["per_ticker"][0]
    assert pt["error"]["reason"] == "insufficient_history"


# ---------- schema stability ----------


def test_per_ticker_shape_stable_across_success_and_failure(cfg: Config, client: HttpClient):
    expected = {
        "ticker", "current_quarter", "prior_quarter",
        "new_positions", "closed_positions",
        "increased_positions", "reduced_positions",
        "error",
    }
    aapl_payload = _resp([
        _holder(name="V", cik="V", share=1_200_000, report_date="2025-12-31", filing_date="2026-02-10"),
        _holder(name="V", cik="V", share=1_000_000, report_date="2025-09-30", filing_date="2025-11-10"),
    ])
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_ticker_response_map({"AAPL": aapl_payload}))
        env = detect_institutional_changes(["AAPL", "BADTKR"], config=cfg, client=client)
    for pt in env["data"]["per_ticker"]:
        assert set(pt.keys()) == expected


# ---------- sort order ----------


def test_increased_sorted_by_change_pct_desc(cfg: Config, client: HttpClient):
    q4 = [
        _holder(name="Small", cik="S", share=150_000, report_date="2025-12-31", filing_date="2026-02-10"),
        _holder(name="Big",   cik="B", share=1_200_000, report_date="2025-12-31", filing_date="2026-02-10"),
    ]
    q3 = [
        _holder(name="Small", cik="S", share=100_000, report_date="2025-09-30", filing_date="2025-11-10"),
        _holder(name="Big",   cik="B", share=1_000_000, report_date="2025-09-30", filing_date="2025-11-10"),
    ]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(q4 + q3))
        env = detect_institutional_changes(["AAPL"], min_change_pct=10, config=cfg, client=client)
    # Small grew 50%, Big grew 20%. Small should come first.
    pt = env["data"]["per_ticker"][0]
    names = [e["name"] for e in pt["increased_positions"]]
    assert names == ["Small", "Big"]


def test_reduced_sorted_most_negative_first(cfg: Config, client: HttpClient):
    q4 = [
        _holder(name="LightCut", cik="L", share=850_000, report_date="2025-12-31", filing_date="2026-02-10"),
        _holder(name="HeavyCut", cik="H", share=200_000, report_date="2025-12-31", filing_date="2026-02-10"),
    ]
    q3 = [
        _holder(name="LightCut", cik="L", share=1_000_000, report_date="2025-09-30", filing_date="2025-11-10"),
        _holder(name="HeavyCut", cik="H", share=1_000_000, report_date="2025-09-30", filing_date="2025-11-10"),
    ]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(q4 + q3))
        env = detect_institutional_changes(["AAPL"], min_change_pct=10, config=cfg, client=client)
    pt = env["data"]["per_ticker"][0]
    names = [e["name"] for e in pt["reduced_positions"]]
    assert names == ["HeavyCut", "LightCut"]  # -80% before -15%


# ---------- input validation ----------


def test_empty_ticker_list_rejected(cfg: Config, client: HttpClient):
    env = detect_institutional_changes([], config=cfg, client=client)
    assert env["status"] == "error"


def test_non_list_tickers_rejected(cfg: Config, client: HttpClient):
    env = detect_institutional_changes("AAPL", config=cfg, client=client)  # type: ignore[arg-type]
    assert env["status"] == "error"


def test_empty_string_in_ticker_list_rejected(cfg: Config, client: HttpClient):
    env = detect_institutional_changes(["AAPL", "  "], config=cfg, client=client)
    assert env["status"] == "error"


def test_too_many_tickers_rejected(cfg: Config, client: HttpClient):
    env = detect_institutional_changes(["T"] * 101, config=cfg, client=client)
    assert env["status"] == "error"


@pytest.mark.parametrize("bad", [0, -1, 1001])
def test_min_change_pct_out_of_range(bad, cfg: Config, client: HttpClient):
    env = detect_institutional_changes(
        ["AAPL"], min_change_pct=bad, config=cfg, client=client,
    )
    assert env["status"] == "error"


@pytest.mark.parametrize("bad", [10.0, "10", None, True])
def test_min_change_pct_non_int_rejected(bad, cfg: Config, client: HttpClient):
    env = detect_institutional_changes(
        ["AAPL"], min_change_pct=bad, config=cfg, client=client,  # type: ignore[arg-type]
    )
    assert env["status"] == "error"
