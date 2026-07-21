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


# ---------------------------------------------------------------------------
# collect group (M1.5)
# ---------------------------------------------------------------------------


def _mock_collect_world(requests_mock):
    requests_mock.get("https://gamma-api.polymarket.com/events", json=[])
    requests_mock.get("https://data-api.polymarket.com/trades", json=[])


def test_collect_run_emits_envelope_and_exit_0(config_file, requests_mock, capsys):
    _mock_collect_world(requests_mock)
    rc = main(["--config", str(config_file), "collect", "run"])
    assert rc == 0
    env = json.loads(capsys.readouterr().out)
    assert env["daemon"] == "consensus_collector"
    assert env["status"] in ("ok", "degraded")
    assert "result" in env and "tape" in env["result"]


def test_collect_run_skipped_when_lock_fresh(config_file, requests_mock, capsys, tmp_path):
    import time
    _mock_collect_world(requests_mock)
    lock = tmp_path / "l2_tape.db.lock"
    lock.write_text(json.dumps({"pid": 999_999, "ts": time.time()}), encoding="utf-8")
    rc = main(["--config", str(config_file), "collect", "run"])
    assert rc == 0
    env = json.loads(capsys.readouterr().out)
    assert env["status"] == "skipped_lock"
    assert lock.exists(), "a fresh foreign lock must not be deleted"


def test_collect_run_steals_stale_lock(config_file, requests_mock, capsys, tmp_path):
    _mock_collect_world(requests_mock)
    lock = tmp_path / "l2_tape.db.lock"
    lock.write_text(json.dumps({"pid": 999_999, "ts": 1.0}), encoding="utf-8")  # ancient
    rc = main(["--config", str(config_file), "collect", "run"])
    assert rc == 0
    env = json.loads(capsys.readouterr().out)
    assert env["status"] in ("ok", "degraded")
    assert not lock.exists(), "our own lock is released after the pass"


def test_collect_run_corrupt_lock_treated_stale(config_file, requests_mock, capsys, tmp_path):
    _mock_collect_world(requests_mock)
    (tmp_path / "l2_tape.db.lock").write_text("not json{", encoding="utf-8")
    rc = main(["--config", str(config_file), "collect", "run"])
    assert rc == 0
    assert json.loads(capsys.readouterr().out)["status"] in ("ok", "degraded")


def test_collect_run_fatal_init_still_emits_envelope(tmp_path, capsys):
    rc = main(["--config", str(tmp_path / "missing.yaml"), "collect", "run"])
    assert rc == 1
    env = json.loads(capsys.readouterr().out)
    assert env["daemon"] == "consensus_collector"
    assert env["status"] == "error"
    assert env["errors"]


def test_collect_run_appends_envelope_log(tmp_path, requests_mock, capsys):
    import copy
    import yaml as _yaml
    from tests.conftest import BASE_CONFIG

    cfg = copy.deepcopy(BASE_CONFIG)
    cfg["collector"]["envelope_log"] = "envelopes.jsonl"
    path = tmp_path / "config.yaml"
    path.write_text(_yaml.safe_dump(cfg), encoding="utf-8")
    _mock_collect_world(requests_mock)
    assert main(["--config", str(path), "collect", "run"]) == 0
    lines = (tmp_path / "envelopes.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["daemon"] == "consensus_collector"


def test_collect_run_envelope_log_failure_degrades_status(tmp_path, requests_mock, capsys):
    import copy
    import yaml as _yaml
    from tests.conftest import BASE_CONFIG

    cfg = copy.deepcopy(BASE_CONFIG)
    cfg["collector"]["envelope_log"] = "elog/envelopes.jsonl"
    path = tmp_path / "config.yaml"
    path.write_text(_yaml.safe_dump(cfg), encoding="utf-8")
    (tmp_path / "elog").write_text("a file where a dir must go", encoding="utf-8")
    _mock_collect_world(requests_mock)
    rc = main(["--config", str(path), "collect", "run"])
    assert rc == 0
    env = json.loads(capsys.readouterr().out)
    # errors present => status is not "ok" (orchestrators branch on status).
    assert env["status"] == "degraded"
    assert any("envelope_log" in e for e in env["errors"])


def test_collect_status_human_and_json(config_file, requests_mock, capsys):
    _mock_collect_world(requests_mock)
    assert main(["--config", str(config_file), "collect", "run"]) == 0
    capsys.readouterr()
    assert main(["--config", str(config_file), "collect", "status"]) == 0
    text = capsys.readouterr().out
    assert "CONSENSUS collector status" in text
    assert main(["--config", str(config_file), "--json", "collect", "status"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["kind"] == "collect.status"
    assert "tape" in out and "tiers" in out


def test_m10_scan_cli_wiring(config_file, requests_mock, capsys):
    _mock_collect_world(requests_mock)
    assert main(["--config", str(config_file), "collect", "run"]) == 0  # create tape
    capsys.readouterr()
    assert main(["--config", str(config_file), "--json", "m10", "scan"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["daemon"] == "consensus_m10"
    assert out["result"]["dossiers"] == []  # empty tape -> nothing surfaces
    assert "not a validated trade signal" in out["caveat"].lower()
    assert main(["--config", str(config_file), "m10", "scan", "--lookback-hours", "24"]) == 0
    assert "M10 UNUSUAL_ACTIVITY scan" in capsys.readouterr().out


def test_collect_supply_readout(config_file, requests_mock, capsys):
    _mock_collect_world(requests_mock)
    assert main(["--config", str(config_file), "collect", "run"]) == 0
    capsys.readouterr()
    assert main(["--config", str(config_file), "--json", "collect", "supply"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["kind"] == "collect.supply"
    assert out["verdict"] in ("SUFFICIENT", "THIN", "EMPTY")
    for key in ("supply", "roster", "window", "window_fills", "reasons"):
        assert key in out
    assert out["verdict"] == "EMPTY"  # empty mocked world -> no fills in window
    # human path renders
    assert main(["--config", str(config_file), "collect", "supply", "--lookback-hours", "48"]) == 0
    text = capsys.readouterr().out
    assert "data-sufficiency readout" in text and "VERDICT:" in text


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
