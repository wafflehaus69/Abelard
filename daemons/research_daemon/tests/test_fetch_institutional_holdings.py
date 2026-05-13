"""fetch_institutional_holdings behaviour — parsing, top-N, quarter grouping."""

from __future__ import annotations

from datetime import date

import pytest
import requests_mock

from research_daemon import fetch_institutional_holdings as fih_module
from research_daemon.config import Config
from research_daemon.fetch_institutional_holdings import (
    FINNHUB_BASE,
    fetch_institutional_holdings,
)
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
    change=5_000,
    portfolio_percent=1.88,
    report_date="2025-12-31",
    filing_date="2026-02-14",
):
    return {
        "name": name,
        "cik": cik,
        "share": share,
        "change": change,
        "filingDate": filing_date,
        "reportDate": report_date,
        "portfolioPercent": portfolio_percent,
        "noVoting": 0,
        "putCallShare": "",
    }


def _resp(holders, symbol="AAPL", cusip="037833100"):
    return {"symbol": symbol, "cusip": cusip, "ownership": holders}


# ---------- happy path ----------


def test_ok_with_holders(cfg: Config, client: HttpClient):
    holders = [
        _holder(name="Vanguard", share=1_500_000, change=5_000),
        _holder(name="BlackRock", share=1_300_000, change=-2_000),
        _holder(name="State Street", share=900_000, change=10_000),
    ]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(holders))
        env = fetch_institutional_holdings("aapl", top_n=5, config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["warnings"] == []

    data = env["data"]
    assert data["ticker"] == "AAPL"
    assert data["top_n"] == 5
    assert data["as_of_quarter"] == "2025Q4"
    assert data["reported_at"] == "2025-12-31T00:00:00Z"
    assert data["holders_returned"] == 3
    assert data["holders_total_in_quarter"] == 3
    assert data["dropped_count"] == 0


def test_default_top_n_is_ten(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["data"]["top_n"] == 10


def test_empty_ownership_is_complete_not_partial(cfg: Config, client: HttpClient):
    """Small-caps often legitimately have no 13F filers. Not a failure."""
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([]))
        env = fetch_institutional_holdings("ARRN", config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["warnings"] == []
    data = env["data"]
    assert data["holders_returned"] == 0
    assert data["holders_total_in_quarter"] == 0
    assert data["dropped_count"] == 0
    assert data["holders"] == []
    assert data["as_of_quarter"] is None
    assert data["reported_at"] is None
    assert data["reported_at_unix"] is None
    assert data["earliest_filed_at"] is None
    assert data["latest_filed_at"] is None


# ---------- sorting and top-N ----------


def test_holders_sorted_by_shares_descending(cfg: Config, client: HttpClient):
    holders = [
        _holder(name="Small Fund", share=100_000),
        _holder(name="Big Fund", share=10_000_000),
        _holder(name="Medium Fund", share=500_000),
    ]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(holders))
        env = fetch_institutional_holdings("AAPL", top_n=5, config=cfg, client=client)
    names = [h["name"] for h in env["data"]["holders"]]
    assert names == ["Big Fund", "Medium Fund", "Small Fund"]


def test_top_n_slices_after_sort(cfg: Config, client: HttpClient):
    holders = [_holder(name=f"F{i}", share=i * 10_000) for i in range(1, 21)]  # 1..20
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(holders))
        env = fetch_institutional_holdings("AAPL", top_n=3, config=cfg, client=client)
    data = env["data"]
    assert data["holders_returned"] == 3
    assert data["holders_total_in_quarter"] == 20
    # Largest three, in descending order.
    names = [h["name"] for h in data["holders"]]
    assert names == ["F20", "F19", "F18"]


# ---------- per-holder schema ----------


def test_per_holder_schema_is_stable(cfg: Config, client: HttpClient):
    expected = {
        "name", "cik", "shares", "shares_change_qoq", "portfolio_percent",
        "filed_at_unix", "filed_at",
    }
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([_holder(), _holder(name="Other", share=200_000)]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)

    for h in env["data"]["holders"]:
        assert set(h.keys()) == expected
        assert isinstance(h["name"], str)
        assert h["cik"] is None or isinstance(h["cik"], str)
        assert isinstance(h["shares"], int)
        assert h["shares_change_qoq"] is None or isinstance(h["shares_change_qoq"], int)
        assert h["portfolio_percent"] is None or isinstance(h["portfolio_percent"], float)
        assert isinstance(h["filed_at_unix"], int)
        assert h["filed_at"].endswith("T00:00:00Z")
    # Internal _report_* keys must be scrubbed.
    for h in env["data"]["holders"]:
        for k in h:
            assert not k.startswith("_")


def test_negative_qoq_change_preserved(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([_holder(change=-50_000)]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["data"]["holders"][0]["shares_change_qoq"] == -50_000


def test_missing_change_becomes_null(cfg: Config, client: HttpClient):
    h = _holder()
    h.pop("change")
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([h]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["data"]["holders"][0]["shares_change_qoq"] is None


def test_missing_cik_becomes_null(cfg: Config, client: HttpClient):
    h = _holder()
    h.pop("cik")
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([h]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["data"]["holders"][0]["cik"] is None


def test_missing_portfolio_percent_becomes_null(cfg: Config, client: HttpClient):
    h = _holder()
    h.pop("portfolioPercent")
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([h]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["data"]["holders"][0]["portfolio_percent"] is None


# ---------- quarter grouping ----------


@pytest.mark.parametrize("report_date,expected_q", [
    ("2025-03-31", "2025Q1"),
    ("2025-06-30", "2025Q2"),
    ("2025-09-30", "2025Q3"),
    ("2025-12-31", "2025Q4"),
    ("2026-02-14", "2026Q1"),
])
def test_quarter_label_mapping(report_date, expected_q, cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([_holder(report_date=report_date)]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["data"]["as_of_quarter"] == expected_q


def test_only_most_recent_quarter_returned(cfg: Config, client: HttpClient):
    """Mixed-quarter payload: only the newest reportDate should make it through."""
    holders = [
        _holder(name="Old Q3 Filer", share=5_000_000, report_date="2025-09-30", filing_date="2025-11-14"),
        _holder(name="New Q4 A",     share=3_000_000, report_date="2025-12-31", filing_date="2026-02-10"),
        _holder(name="New Q4 B",     share=1_000_000, report_date="2025-12-31", filing_date="2026-02-13"),
    ]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(holders))
        env = fetch_institutional_holdings("AAPL", top_n=10, config=cfg, client=client)

    data = env["data"]
    assert data["as_of_quarter"] == "2025Q4"
    assert data["holders_total_in_quarter"] == 2
    names = [h["name"] for h in data["holders"]]
    assert "Old Q3 Filer" not in names
    assert names == ["New Q4 A", "New Q4 B"]


def test_filing_date_range_spans_all_quarter_items(cfg: Config, client: HttpClient):
    """earliest/latest_filed_at cover the entire quarter slice, not just top-N."""
    holders = [
        _holder(name=f"F{i}", share=100_000 + i, filing_date=f"2026-02-{10 + i:02d}")
        for i in range(5)  # 2026-02-10 .. 2026-02-14
    ]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(holders))
        env = fetch_institutional_holdings("AAPL", top_n=2, config=cfg, client=client)
    data = env["data"]
    assert data["earliest_filed_at"] == "2026-02-10T00:00:00Z"
    assert data["latest_filed_at"] == "2026-02-14T00:00:00Z"
    assert data["holders_returned"] == 2
    assert data["holders_total_in_quarter"] == 5


# ---------- drop behaviour ----------


def test_missing_name_dropped(cfg: Config, client: HttpClient):
    bad = _holder()
    bad.pop("name")
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([_holder(), bad]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["data"]["dropped_count"] == 1
    assert env["data_completeness"] == "partial"
    assert env["warnings"][0]["reason"] == "parse_error"
    assert env["warnings"][0]["field"] == "holders"


@pytest.mark.parametrize("field", ["share", "reportDate", "filingDate"])
def test_other_required_fields_dropped(field, cfg: Config, client: HttpClient):
    bad = _holder()
    bad.pop(field)
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([_holder(), bad]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["data"]["dropped_count"] == 1


@pytest.mark.parametrize("bad_share", [0, -100])
def test_non_positive_share_dropped(bad_share, cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([_holder(share=bad_share)]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["data"]["holders_returned"] == 0
    assert env["data"]["dropped_count"] == 1


def test_non_dict_items_dropped(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([_holder(), "junk", None, 42]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["data"]["holders_returned"] == 1
    assert env["data"]["dropped_count"] == 3


def test_invalid_report_date_dropped(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([_holder(report_date="not-a-date")]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["data"]["dropped_count"] == 1


# ---------- upstream failures ----------


def test_wrong_payload_shape_is_error(cfg: Config, client: HttpClient):
    """A raw list at the top level is not the expected envelope shape."""
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=[_holder()])
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["status"] == "error"


def test_missing_ownership_key_is_error(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json={"symbol": "AAPL"})
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["status"] == "error"


def test_404_is_not_found(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, status_code=404)
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["status"] == "not_found"


def test_429_is_rate_limited(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, status_code=429)
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["status"] == "rate_limited"


def test_500_is_error(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, status_code=500)
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    assert env["status"] == "error"


# ---------- input validation ----------


def test_empty_ticker_rejected(cfg: Config, client: HttpClient):
    env = fetch_institutional_holdings("  ", config=cfg, client=client)
    assert env["status"] == "error"


def test_non_string_ticker_rejected(cfg: Config, client: HttpClient):
    env = fetch_institutional_holdings(None, config=cfg, client=client)  # type: ignore[arg-type]
    assert env["status"] == "error"


@pytest.mark.parametrize("bad_n", [0, -1, 101, 10_000])
def test_top_n_out_of_range_rejected(bad_n, cfg: Config, client: HttpClient):
    env = fetch_institutional_holdings("AAPL", top_n=bad_n, config=cfg, client=client)
    assert env["status"] == "error"
    assert "top_n" in env["error_detail"]


@pytest.mark.parametrize("bad_n", [1.5, "5", None, True])
def test_top_n_non_int_rejected(bad_n, cfg: Config, client: HttpClient):
    env = fetch_institutional_holdings("AAPL", top_n=bad_n, config=cfg, client=client)  # type: ignore[arg-type]
    assert env["status"] == "error"


# ---------- num_quarters variant ----------


def test_num_quarters_one_is_default_and_flat(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([_holder()]))
        env = fetch_institutional_holdings("AAPL", config=cfg, client=client)
    data = env["data"]
    # Flat shape — single quarter fields live at top level.
    assert "quarters" not in data
    assert "as_of_quarter" in data
    assert "holders" in data


def test_num_quarters_two_returns_list_shape(cfg: Config, client: HttpClient):
    holders = [
        _holder(name="Q4 Big",   share=2_000_000, report_date="2025-12-31", filing_date="2026-02-10"),
        _holder(name="Q4 Small", share=500_000,   report_date="2025-12-31", filing_date="2026-02-12"),
        _holder(name="Q3 Big",   share=1_800_000, report_date="2025-09-30", filing_date="2025-11-10"),
    ]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(holders))
        env = fetch_institutional_holdings(
            "AAPL", top_n=10, num_quarters=2, config=cfg, client=client,
        )

    assert env["status"] == "ok"
    data = env["data"]
    assert "quarters" in data
    assert data["num_quarters_requested"] == 2
    assert data["quarters_returned"] == 2
    # Flat fields should NOT be present on multi-quarter shape.
    assert "as_of_quarter" not in data
    assert "holders" not in data
    assert "reported_at" not in data

    # Most-recent-first ordering.
    assert data["quarters"][0]["as_of_quarter"] == "2025Q4"
    assert data["quarters"][1]["as_of_quarter"] == "2025Q3"

    # Each quarter sub-dict has the per-quarter fields.
    q0 = data["quarters"][0]
    assert q0["holders_returned"] == 2
    assert q0["holders_total_in_quarter"] == 2
    names = [h["name"] for h in q0["holders"]]
    assert names == ["Q4 Big", "Q4 Small"]


def test_num_quarters_two_but_only_one_quarter_available(cfg: Config, client: HttpClient):
    """Only one quarter of data — quarters list has length 1, not padded."""
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([_holder()]))
        env = fetch_institutional_holdings(
            "AAPL", num_quarters=2, config=cfg, client=client,
        )
    data = env["data"]
    assert data["num_quarters_requested"] == 2
    assert data["quarters_returned"] == 1
    assert len(data["quarters"]) == 1
    assert data["quarters"][0]["as_of_quarter"] == "2025Q4"


def test_num_quarters_two_with_empty_ownership(cfg: Config, client: HttpClient):
    """Empty ownership → complete with empty quarters list."""
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp([]))
        env = fetch_institutional_holdings(
            "ARRN", num_quarters=2, config=cfg, client=client,
        )
    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["data"]["quarters"] == []
    assert env["data"]["quarters_returned"] == 0


def test_num_quarters_higher_selects_most_recent_n(cfg: Config, client: HttpClient):
    holders = [
        _holder(share=100, report_date="2025-03-31", filing_date="2025-05-10"),
        _holder(share=200, report_date="2025-06-30", filing_date="2025-08-10"),
        _holder(share=300, report_date="2025-09-30", filing_date="2025-11-10"),
        _holder(share=400, report_date="2025-12-31", filing_date="2026-02-10"),
    ]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(holders))
        env = fetch_institutional_holdings(
            "AAPL", num_quarters=3, config=cfg, client=client,
        )
    qs = env["data"]["quarters"]
    labels = [q["as_of_quarter"] for q in qs]
    # Most-recent-first; oldest (Q1) dropped.
    assert labels == ["2025Q4", "2025Q3", "2025Q2"]


def test_num_quarters_top_n_applied_per_quarter(cfg: Config, client: HttpClient):
    holders = [
        _holder(name=f"Q4-{i}", share=i * 1_000, report_date="2025-12-31", filing_date="2026-02-10")
        for i in range(1, 6)  # 5 Q4 holders
    ] + [
        _holder(name=f"Q3-{i}", share=i * 1_000, report_date="2025-09-30", filing_date="2025-11-10")
        for i in range(1, 4)  # 3 Q3 holders
    ]
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json=_resp(holders))
        env = fetch_institutional_holdings(
            "AAPL", top_n=2, num_quarters=2, config=cfg, client=client,
        )
    qs = env["data"]["quarters"]
    assert qs[0]["holders_returned"] == 2
    assert qs[0]["holders_total_in_quarter"] == 5
    assert qs[1]["holders_returned"] == 2
    assert qs[1]["holders_total_in_quarter"] == 3


@pytest.mark.parametrize("bad", [0, -1, 9, 100])
def test_num_quarters_out_of_range_rejected(bad, cfg: Config, client: HttpClient):
    env = fetch_institutional_holdings(
        "AAPL", num_quarters=bad, config=cfg, client=client,
    )
    assert env["status"] == "error"
    assert "num_quarters" in env["error_detail"]


@pytest.mark.parametrize("bad", [1.5, "2", None, True])
def test_num_quarters_non_int_rejected(bad, cfg: Config, client: HttpClient):
    env = fetch_institutional_holdings(
        "AAPL", num_quarters=bad, config=cfg, client=client,  # type: ignore[arg-type]
    )
    assert env["status"] == "error"
