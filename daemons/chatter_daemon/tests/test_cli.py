"""CLI — load/validate, --all, fail-loud, and the bundled barber_growth list."""

from __future__ import annotations

import json

import pytest

from chatter_daemon.cli import main


def _run(argv, capsys):
    rc = main(argv)
    out = capsys.readouterr().out.strip().splitlines()[-1]
    return rc, json.loads(out)


def test_cli_watchlist_loads(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    rc, payload = _run(["--watchlist", "alpha"], capsys)
    assert rc == 0
    assert payload["errors"] == []
    assert payload["canonical_ts"] is not None
    assert {w["label"] for w in payload["windows"]} == {"24h", "7d", "monthly"}
    assert payload["watchlists"][0]["name"] == "alpha"
    assert payload["records"] == []


def test_cli_all_enumerates(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    rc, payload = _run(["--all"], capsys)
    assert rc == 0
    assert [w["name"] for w in payload["watchlists"]] == ["alpha", "beta"]


def test_cli_missing_list_fails_loud(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    rc, payload = _run(["--watchlist", "nope"], capsys)
    assert rc == 1
    assert payload["errors"]
    assert payload["errors"][0].startswith("watchlist: watchlist not found")


def test_cli_requires_a_target(capsys):
    # The --watchlist / --all group is required and mutually exclusive.
    with pytest.raises(SystemExit):
        main([])


def test_cli_bundled_barber_growth_validates(capsys):
    # Hermetic (no network): loads the REAL bundled watchlist via its default dir.
    rc, payload = _run(["--watchlist", "barber_growth"], capsys)
    assert rc == 0
    assert payload["watchlists"][0]["name"] == "barber_growth"
    assert payload["watchlists"][0]["tickers"] == 46
    assert payload["watchlists"][0]["active"] == 45  # P excluded (enabled=false)
