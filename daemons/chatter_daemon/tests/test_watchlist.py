"""Watchlist config primitive — load, validate, fail-loud, --all."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chatter_daemon.config import _default_watchlists_dir
from chatter_daemon.watchlist import (
    WatchlistError,
    load_all_watchlists,
    load_watchlist,
    write_watchlist_csv,
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


# --- CSV portfolio form (Order 21) — the human-editable, spreadsheet-round-trip format -------

_CSV = (
    "symbol,names,name_match,is_etf,enabled,ambiguous_name,notes\n"
    "NVDA,,true,false,true,false,\n"
    "MU,Micron,false,false,true,false,collision-word; ticker-only\n"
    "TSM,Taiwan Semiconductor|TSMC|Taiwan Semi,true,false,true,false,\n"
    "BAI,,false,true,true,false,ETF — no clean term\n"
    "PLM,,false,false,false,false,pending symbol confirmation\n"
)


def _write_csv(directory: Path, name: str, text: str) -> Path:
    (directory / f"{name}.csv").write_text(text, encoding="utf-8")
    return directory


def test_csv_parses_fields_and_defaults(tmp_path):
    wl = load_watchlist("pf", watchlists_dir=_write_csv(tmp_path, "pf", _CSV))
    by = {t.symbol: t for t in wl.tickers}
    assert wl.name == "pf" and len(wl.tickers) == 5 and len(wl.active_tickers) == 4  # PLM disabled
    assert by["TSM"].names == ["Taiwan Semiconductor", "TSMC", "Taiwan Semi"]  # pipe-split
    assert by["MU"].name_match is False and by["MU"].notes == "collision-word; ticker-only"
    assert by["BAI"].is_etf is True and by["PLM"].enabled is False
    assert by["NVDA"].name_match is True and by["NVDA"].names == []  # blank cells -> defaults


def test_csv_round_trips_through_write(tmp_path):
    wl = load_watchlist("pf", watchlists_dir=_write_csv(tmp_path, "pf", _CSV))
    write_watchlist_csv(wl, tmp_path / "again.csv")
    wl2 = load_watchlist("again", watchlists_dir=tmp_path)
    assert [t.model_dump() for t in wl.tickers] == [t.model_dump() for t in wl2.tickers]


def test_csv_missing_header_fails_loud(tmp_path):
    (tmp_path / "bad.csv").write_text("NVDA,,true\nMU,,true\n", encoding="utf-8")  # no header row
    with pytest.raises(WatchlistError, match="header"):
        load_watchlist("bad", watchlists_dir=tmp_path)


def test_csv_bad_symbol_fails_loud(tmp_path):
    _write_csv(tmp_path, "bad", "symbol,names\nnot a ticker,\n")
    with pytest.raises(WatchlistError, match="validation failed"):
        load_watchlist("bad", watchlists_dir=tmp_path)


def test_both_json_and_csv_is_ambiguous(tmp_path):
    _write_csv(tmp_path, "pf", _CSV)
    (tmp_path / "pf.json").write_text('{"name":"pf","tickers":[{"symbol":"NVDA"}]}', encoding="utf-8")
    with pytest.raises(WatchlistError, match="ambiguous"):
        load_watchlist("pf", watchlists_dir=tmp_path)


def test_load_all_mixes_csv_and_json(tmp_path):
    _write_csv(tmp_path, "alpha", _CSV)
    (tmp_path / "beta.json").write_text('{"name":"beta","tickers":[{"symbol":"NVDA"}]}', encoding="utf-8")
    assert {w.name for w in load_all_watchlists(tmp_path)} == {"alpha", "beta"}


def test_load_all_collision_across_formats_fails_loud(tmp_path):
    _write_csv(tmp_path, "pf", _CSV)
    (tmp_path / "pf.json").write_text('{"name":"pf","tickers":[{"symbol":"NVDA"}]}', encoding="utf-8")
    with pytest.raises(WatchlistError):
        load_all_watchlists(tmp_path)


def test_bundled_barber_growth_csv_loads():
    # The migrated portfolio: 46 tickers, 45 active (P disabled), CSV-sourced (Order 21).
    wl = load_watchlist("barber_growth", watchlists_dir=_default_watchlists_dir())
    assert wl.name == "barber_growth"
    assert len(wl.tickers) == 46 and len(wl.active_tickers) == 45
    assert {"NVDA", "MU", "META", "MSFT"} <= {t.symbol for t in wl.tickers}
