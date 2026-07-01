"""detect_institutional_changes behaviour — yfinance-backed QoQ deltas."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from research_daemon import fetch_institutional_holdings as fih_module
from research_daemon.config import Config
from research_daemon.detect_institutional_changes import detect_institutional_changes
from research_daemon.http_client import HttpClient


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
        "Date Reported": datetime.fromisoformat(date_reported),
        "Holder": holder,
        "pctHeld": pct_held,
        "Shares": shares,
        "Value": value,
        "pctChange": pct_change,
    }


class _TickerRouter:
    """Route yf.Ticker(symbol) to per-symbol (inst, mf) payloads."""
    def __init__(self, payloads: dict[str, tuple[list | None, list | None]]):
        self.payloads = payloads

    def __call__(self, symbol: str):
        inst, mf = self.payloads.get(symbol.upper(), (None, None))
        instance = MagicMock()
        instance.institutional_holders = inst
        instance.mutualfund_holders = mf
        return instance


@pytest.fixture
def route_tickers(monkeypatch):
    def _apply(payloads):
        router = _TickerRouter(payloads)
        monkeypatch.setattr(fih_module.yf, "Ticker", router)
    return _apply


# ---------- happy path ----------


def test_increased_and_reduced_buckets_populated(cfg: Config, client: HttpClient, route_tickers):
    route_tickers({
        "AAPL": (
            [
                _row(holder="BigBuyer", shares=1_200_000, pct_change=0.20),   # +20%
                _row(holder="SmallBuyer", shares=1_050_000, pct_change=0.05),  # +5%, below default
                _row(holder="BigSeller", shares=800_000, pct_change=-0.20),   # -20%
                _row(holder="Steady", shares=1_000_000, pct_change=0.001),    # ~0%
            ],
            None,
        ),
    })
    env = detect_institutional_changes(["AAPL"], min_change_pct=10, config=cfg, client=client)

    assert env["status"] == "ok"
    pt = env["data"]["per_ticker"][0]
    assert pt["error"] is None
    assert pt["current_quarter"] == "2026Q1"
    assert pt["prior_quarter"] is None

    inc_names = [e["name"] for e in pt["increased_positions"]]
    red_names = [e["name"] for e in pt["reduced_positions"]]
    assert inc_names == ["BigBuyer"]
    assert red_names == ["BigSeller"]
    assert pt["increased_positions"][0]["change_pct"] == 20.0
    assert pt["reduced_positions"][0]["change_pct"] == -20.0


def test_new_and_closed_always_empty(cfg: Config, client: HttpClient, route_tickers):
    """No matter what the data looks like, new/closed can't be derived from yfinance."""
    route_tickers({
        "AAPL": (
            [_row(holder="Anyone", shares=500_000, pct_change=0.50)],
            None,
        ),
    })
    env = detect_institutional_changes(["AAPL"], config=cfg, client=client)
    pt = env["data"]["per_ticker"][0]
    assert pt["new_positions"] == []
    assert pt["closed_positions"] == []


def test_standing_insufficient_history_warning_always_present(
    cfg: Config, client: HttpClient, route_tickers
):
    route_tickers({
        "AAPL": ([_row(pct_change=0.30)], None),
    })
    env = detect_institutional_changes(["AAPL"], config=cfg, client=client)
    insuff = [w for w in env["warnings"] if w["reason"] == "insufficient_history"]
    assert len(insuff) == 1
    assert "new_positions" in insuff[0]["field"]
    assert "closed_positions" in insuff[0]["field"]
    assert env["data"]["source_supports"]["new_and_closed_detection"] is False
    assert env["data"]["source_supports"]["increased_and_reduced_detection"] is True


def test_completeness_is_partial_by_design(cfg: Config, client: HttpClient, route_tickers):
    """Even in a fully-successful call, completeness is partial because the
    new/closed buckets are inherently unfilled by this source."""
    route_tickers({"AAPL": ([_row(pct_change=0.20)], None)})
    env = detect_institutional_changes(["AAPL"], config=cfg, client=client)
    assert env["data_completeness"] == "partial"


# ---------- threshold ----------


def test_min_change_pct_boundary(cfg: Config, client: HttpClient, route_tickers):
    route_tickers({
        "AAPL": (
            [
                _row(holder="A", pct_change=0.10),   # exactly 10%; abs >= threshold
                _row(holder="B", pct_change=0.099),  # 9.9%; below
            ],
            None,
        ),
    })
    env = detect_institutional_changes(["AAPL"], min_change_pct=10, config=cfg, client=client)
    pt = env["data"]["per_ticker"][0]
    names = [e["name"] for e in pt["increased_positions"]]
    assert names == ["A"]


def test_higher_threshold_filters_more(cfg: Config, client: HttpClient, route_tickers):
    route_tickers({
        "AAPL": (
            [
                _row(holder="Moderate", pct_change=0.15),
                _row(holder="Aggressive", pct_change=0.50),
            ],
            None,
        ),
    })
    env = detect_institutional_changes(
        ["AAPL"], min_change_pct=25, config=cfg, client=client,
    )
    pt = env["data"]["per_ticker"][0]
    names = [e["name"] for e in pt["increased_positions"]]
    assert names == ["Aggressive"]


def test_holders_missing_pct_change_are_skipped(cfg: Config, client: HttpClient, route_tickers):
    row = _row()
    row.pop("pctChange")
    route_tickers({"AAPL": ([row], None)})
    env = detect_institutional_changes(["AAPL"], config=cfg, client=client)
    pt = env["data"]["per_ticker"][0]
    assert pt["increased_positions"] == []
    assert pt["reduced_positions"] == []


# ---------- sort order ----------


def test_increased_sorted_by_change_pct_desc(cfg: Config, client: HttpClient, route_tickers):
    route_tickers({
        "AAPL": (
            [
                _row(holder="Small", pct_change=0.15),
                _row(holder="Big",   pct_change=0.50),
                _row(holder="Med",   pct_change=0.30),
            ],
            None,
        ),
    })
    env = detect_institutional_changes(["AAPL"], min_change_pct=10, config=cfg, client=client)
    names = [e["name"] for e in env["data"]["per_ticker"][0]["increased_positions"]]
    assert names == ["Big", "Med", "Small"]


def test_reduced_sorted_most_negative_first(cfg: Config, client: HttpClient, route_tickers):
    route_tickers({
        "AAPL": (
            [
                _row(holder="LightCut", pct_change=-0.15),
                _row(holder="HeavyCut", pct_change=-0.60),
            ],
            None,
        ),
    })
    env = detect_institutional_changes(["AAPL"], min_change_pct=10, config=cfg, client=client)
    names = [e["name"] for e in env["data"]["per_ticker"][0]["reduced_positions"]]
    assert names == ["HeavyCut", "LightCut"]


# ---------- multi-ticker ----------


def test_multi_ticker_all_succeed(cfg: Config, client: HttpClient, route_tickers):
    route_tickers({
        "AAPL": ([_row(holder="A", pct_change=0.20)], None),
        "MSFT": ([_row(holder="M", pct_change=0.30)], None),
    })
    env = detect_institutional_changes(["AAPL", "MSFT"], config=cfg, client=client)
    assert env["data"]["tickers_analyzed"] == 2
    assert env["data"]["tickers_failed"] == 0
    tickers = {pt["ticker"] for pt in env["data"]["per_ticker"]}
    assert tickers == {"AAPL", "MSFT"}
    for pt in env["data"]["per_ticker"]:
        assert pt["error"] is None
        assert len(pt["increased_positions"]) == 1


def test_multi_ticker_partial_failure(cfg: Config, client: HttpClient, monkeypatch):
    """One ticker's yf.Ticker call raises; the other succeeds."""
    def router(symbol):
        if symbol.upper() == "BADTKR":
            raise ConnectionError("Yahoo blocked")
        instance = MagicMock()
        instance.institutional_holders = [_row(holder="G", pct_change=0.20)]
        instance.mutualfund_holders = None
        return instance
    monkeypatch.setattr(fih_module.yf, "Ticker", router)

    env = detect_institutional_changes(
        ["AAPL", "BADTKR"], config=cfg, client=client,
    )
    assert env["status"] == "ok"
    assert env["data_completeness"] == "partial"
    assert env["data"]["tickers_analyzed"] == 1
    assert env["data"]["tickers_failed"] == 1

    by_ticker = {pt["ticker"]: pt for pt in env["data"]["per_ticker"]}
    assert by_ticker["AAPL"]["error"] is None
    assert by_ticker["BADTKR"]["error"] is not None
    assert by_ticker["BADTKR"]["error"]["reason"] == "error"
    assert "Yahoo blocked" in by_ticker["BADTKR"]["error"]["detail"]

    upstream_warnings = [w for w in env["warnings"] if w["reason"] == "upstream_error"]
    assert len(upstream_warnings) == 1


# ---------- schema stability ----------


def test_per_ticker_shape_stable_across_success_and_failure(
    cfg: Config, client: HttpClient, monkeypatch
):
    expected = {
        "ticker", "current_quarter", "prior_quarter",
        "new_positions", "closed_positions",
        "increased_positions", "reduced_positions",
        "error",
    }

    def router(symbol):
        if symbol.upper() == "BADTKR":
            raise ConnectionError("boom")
        inst = MagicMock()
        inst.institutional_holders = [_row(pct_change=0.20)]
        inst.mutualfund_holders = None
        return inst
    monkeypatch.setattr(fih_module.yf, "Ticker", router)

    env = detect_institutional_changes(["AAPL", "BADTKR"], config=cfg, client=client)
    for pt in env["data"]["per_ticker"]:
        assert set(pt.keys()) == expected


def test_entry_schema(cfg: Config, client: HttpClient, route_tickers):
    expected = {"name", "cik", "holder_type", "current_shares",
                "shares_change_qoq", "change_pct"}
    route_tickers({"AAPL": ([_row(pct_change=0.20)], None)})
    env = detect_institutional_changes(["AAPL"], config=cfg, client=client)
    entry = env["data"]["per_ticker"][0]["increased_positions"][0]
    assert set(entry.keys()) == expected


# ---------- ticker with zero activity ----------


def test_ticker_with_no_holders_is_clean_success(
    cfg: Config, client: HttpClient, route_tickers
):
    route_tickers({"ARRN": (None, None)})
    env = detect_institutional_changes(["ARRN"], config=cfg, client=client)
    pt = env["data"]["per_ticker"][0]
    assert pt["error"] is None
    assert pt["increased_positions"] == []
    assert pt["reduced_positions"] == []


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
