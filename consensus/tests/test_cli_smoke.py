"""End-to-end: the `consensus data smoke` acceptance, all-OK and gap paths."""

from __future__ import annotations

import json

from consensus.cli import main


def _mock_all(requests_mock, *, kalshi_status=200):
    from tests.conftest import GAMMA_MARKETS, KALSHI_ENV, POSITIONS, TRADES

    requests_mock.get("https://data-api.polymarket.com/trades", json=TRADES)
    requests_mock.get("https://data-api.polymarket.com/positions", json=POSITIONS)
    requests_mock.get("https://gamma-api.polymarket.com/markets", json=GAMMA_MARKETS)
    if kalshi_status == 200:
        requests_mock.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets", json=KALSHI_ENV
        )
    else:
        requests_mock.get(
            "https://api.elections.kalshi.com/trade-api/v2/markets", status_code=kalshi_status
        )


def test_smoke_all_ok(config_file, requests_mock, capsys):
    _mock_all(requests_mock)
    rc = main(["--config", str(config_file), "--json", "data", "smoke"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["kind"] == "data.smoke"
    assert out["all_ok"] is True
    assert out["ok_count"] == out["total"] == 5
    # Cache was populated from the five upstream calls.
    assert out["cache_rows_after"] > out["cache_rows_before"] == 0
    labels = {s["label"]: s["status"] for s in out["steps"]}
    assert labels == {
        "market_meta": "ok", "market_trades": "ok", "wallet_trades": "ok",
        "wallet_positions": "ok", "kalshi_markets": "ok",
    }


def test_smoke_gap_sets_exit_1_but_reports_other_sources(config_file, requests_mock, capsys):
    _mock_all(requests_mock, kalshi_status=500)
    rc = main(["--config", str(config_file), "--json", "data", "smoke"])
    assert rc == 1  # a gap makes the run fail for cron/scripts
    out = json.loads(capsys.readouterr().out)
    assert out["all_ok"] is False
    assert out["ok_count"] == 4
    steps = {s["label"]: s for s in out["steps"]}
    assert steps["kalshi_markets"]["status"] == "error"
    assert steps["kalshi_markets"]["error"]  # a non-empty, structured gap message
    # The other four still succeeded — the gap did not hide them.
    assert steps["market_trades"]["status"] == "ok"


def test_smoke_human_output_renders(config_file, requests_mock, capsys):
    _mock_all(requests_mock)
    rc = main(["--config", str(config_file), "data", "smoke"])
    assert rc == 0
    text = capsys.readouterr().out
    assert "CONSENSUS data smoke" in text
    assert "RESULT: 5/5 sources OK" in text


def test_smoke_json_values_are_structured_not_reprs(config_file, requests_mock, capsys):
    """--json step values must be real JSON structures (dicts with count/sample),
    never a Python repr string of a dataclass."""
    _mock_all(requests_mock)
    rc = main(["--config", str(config_file), "--json", "data", "smoke"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    values = {s["label"]: s["value"] for s in out["steps"]}
    assert isinstance(values["market_meta"], dict)
    assert values["market_meta"]["outcomes"] == ["Yes", "No"]
    assert isinstance(values["market_trades"], dict)
    assert values["market_trades"]["count"] == 2
    assert isinstance(values["market_trades"]["sample"], list)
    assert values["market_trades"]["sample"][0]["price"] in (0.2299998363, 0.49)
    # No repr leakage anywhere in the payload.
    raw_text = json.dumps(out)
    assert "Trade(" not in raw_text and "MarketMeta(" not in raw_text


# ---------------------------------------------------------------------------
# direct inspection commands
# ---------------------------------------------------------------------------


def test_direct_trades_market_json(config_file, requests_mock, capsys):
    from tests.conftest import TRADES

    requests_mock.get("https://data-api.polymarket.com/trades", json=TRADES)
    rc = main(["--config", str(config_file), "--json", "data", "trades", "--market", "0xCID"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["kind"] == "data.trades"
    assert out["count"] == 2
    assert out["trades"][0]["side"] in ("BUY", "SELL")


def test_direct_market_json(config_file, requests_mock, capsys):
    from tests.conftest import GAMMA_MARKETS

    requests_mock.get("https://gamma-api.polymarket.com/markets", json=GAMMA_MARKETS)
    rc = main(["--config", str(config_file), "--json", "data", "market", "--market", "0xCID"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["found"] is True
    assert out["meta"]["outcomes"] == ["Yes", "No"]


def test_direct_kalshi_json(config_file, requests_mock, capsys):
    from tests.conftest import KALSHI_ENV

    requests_mock.get("https://api.elections.kalshi.com/trade-api/v2/markets", json=KALSHI_ENV)
    rc = main(["--config", str(config_file), "--json", "data", "kalshi", "--limit", "5"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["count"] == 1
    assert out["markets"][0]["ticker"] == "KXTEST-1"


def test_direct_command_hard_failure_exits_1_with_structured_error(
    config_file, requests_mock, capsys
):
    requests_mock.get("https://data-api.polymarket.com/trades", status_code=500)
    rc = main(["--config", str(config_file), "--json", "data", "trades", "--market", "0xCID"])
    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["kind"] == "error"
    assert "data_layer.polymarket_data" in out["error"]


def test_config_error_exits_1(tmp_path, capsys):
    rc = main(["--config", str(tmp_path / "missing.yaml"), "data", "smoke"])
    assert rc == 1


def test_cache_stats_command(config_file, requests_mock, capsys):
    from tests.conftest import TRADES

    requests_mock.get("https://data-api.polymarket.com/trades", json=TRADES)
    # Populate the cache with one fetch first.
    assert main(["--config", str(config_file), "--json", "data", "trades", "--market", "0xCID"]) == 0
    capsys.readouterr()
    rc = main(["--config", str(config_file), "--json", "data", "cache-stats"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["kind"] == "data.cache_stats"
    assert out["total_rows"] == 1
    assert out["sources"][0]["source"] == "polymarket_data"


def test_trades_all_walks_pages_and_reports_range(config_file, requests_mock, capsys):
    def mk(i):
        return {"proxyWallet": f"0x{i:x}", "side": "BUY", "conditionId": "0xCID",
                "size": 1.0, "price": 0.5, "timestamp": 1000 + i}

    # page_size = --limit = 2: full page, then short page -> 3 records total.
    requests_mock.get(
        "https://data-api.polymarket.com/trades",
        [{"json": [mk(0), mk(1)]}, {"json": [mk(2)]}],
    )
    rc = main(["--config", str(config_file), "--json", "data", "trades",
               "--market", "0xCID", "--all", "--limit", "2"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["paginated"] is True
    assert out["count"] == 3
    assert out["earliest_ts"] == 1000 and out["latest_ts"] == 1002
    assert out["rate_limit_hits"] == 0
    assert out["sample_size"] == 3
