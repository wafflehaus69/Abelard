"""Watchlist config primitive — load, validate, fail-loud, --all."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chatter_daemon.watchlist import (
    WatchlistError,
    load_all_watchlists,
    load_watchlist,
)


def _write(directory: Path, name: str, text: str) -> Path:
    path = directory / f"{name}.json"
    path.write_text(text, encoding="utf-8")
    return path


def test_valid_load(watchlists_dir):
    wl = load_watchlist("alpha", watchlists_dir=watchlists_dir)
    assert wl.name == "alpha"
    assert [t.symbol for t in wl.tickers] == ["NVDA", "AMD"]
    assert wl.tickers[0].name_match is True  # default
    assert wl.tickers[1].name_match is False


def test_missing_dir(tmp_path):
    with pytest.raises(WatchlistError, match="directory not found"):
        load_watchlist("x", watchlists_dir=tmp_path / "nope")


def test_missing_list(watchlists_dir):
    with pytest.raises(WatchlistError, match="watchlist not found"):
        load_watchlist("does_not_exist", watchlists_dir=watchlists_dir)


def test_malformed_json(tmp_path):
    d = tmp_path / "w"
    d.mkdir()
    _write(d, "bad", "{not valid json")
    with pytest.raises(WatchlistError, match="invalid JSON"):
        load_watchlist("bad", watchlists_dir=d)


def test_non_object_root(tmp_path):
    d = tmp_path / "w"
    d.mkdir()
    _write(d, "arr", "[1, 2, 3]")
    with pytest.raises(WatchlistError, match="must be an object"):
        load_watchlist("arr", watchlists_dir=d)


def test_empty_tickers(tmp_path):
    d = tmp_path / "w"
    d.mkdir()
    _write(d, "empty", json.dumps({"name": "empty", "tickers": []}))
    with pytest.raises(WatchlistError, match="validation failed"):
        load_watchlist("empty", watchlists_dir=d)


def test_schema_violation_extra_field(tmp_path):
    d = tmp_path / "w"
    d.mkdir()
    _write(d, "x", json.dumps({"name": "x", "tickers": [{"symbol": "NVDA", "bogus": 1}]}))
    with pytest.raises(WatchlistError, match="validation failed"):
        load_watchlist("x", watchlists_dir=d)


def test_bad_symbol_format(tmp_path):
    d = tmp_path / "w"
    d.mkdir()
    _write(d, "x", json.dumps({"name": "x", "tickers": [{"symbol": "toolong"}]}))
    with pytest.raises(WatchlistError, match="validation failed"):
        load_watchlist("x", watchlists_dir=d)


def test_name_stem_mismatch(tmp_path):
    d = tmp_path / "w"
    d.mkdir()
    _write(d, "file_name", json.dumps({"name": "other_name", "tickers": [{"symbol": "NVDA"}]}))
    with pytest.raises(WatchlistError, match="does not match filename stem"):
        load_watchlist("file_name", watchlists_dir=d)


def test_duplicate_symbols(tmp_path):
    d = tmp_path / "w"
    d.mkdir()
    _write(d, "x", json.dumps({"name": "x", "tickers": [{"symbol": "NVDA"}, {"symbol": "NVDA"}]}))
    with pytest.raises(WatchlistError, match="validation failed"):
        load_watchlist("x", watchlists_dir=d)


def test_load_all_sorted(watchlists_dir):
    lists = load_all_watchlists(watchlists_dir)
    assert [w.name for w in lists] == ["alpha", "beta"]


def test_load_all_empty_dir(tmp_path):
    d = tmp_path / "w"
    d.mkdir()
    with pytest.raises(WatchlistError, match="no watchlist files"):
        load_all_watchlists(d)


def test_active_tickers_excludes_disabled(watchlists_dir):
    beta = load_watchlist("beta", watchlists_dir=watchlists_dir)
    assert len(beta.tickers) == 2
    assert [t.symbol for t in beta.active_tickers] == ["TSM"]  # P enabled=False excluded
