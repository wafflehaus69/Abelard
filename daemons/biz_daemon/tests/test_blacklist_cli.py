"""blacklist maintenance: add/remove file helpers + CLI subcommands."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from abelard_common import ticker_noise as blacklist
from biz_daemon.cli import main


def _seed(tmp_path: Path) -> Path:
    p = tmp_path / "deny.txt"
    p.write_text("# seed\nFUD\nEOD\n", encoding="utf-8")
    return p


# --- module helpers ----------------------------------------------------------


def test_add_persists_uppercases_dedupes(tmp_path):
    path = _seed(tmp_path)
    added, skipped = blacklist.add_tokens(path, ["jannies", "JANNIES", "wagmi"])
    assert added == ["JANNIES", "WAGMI"]  # dupe input collapsed, uppercased
    assert skipped == []
    loaded = blacklist.load_blacklist(path)
    assert {"JANNIES", "WAGMI"} <= loaded
    assert {"FUD", "EOD"} <= loaded  # seed preserved


def test_add_skips_already_present(tmp_path):
    path = _seed(tmp_path)
    added, skipped = blacklist.add_tokens(path, ["fud", "NEWONE"])
    assert added == ["NEWONE"]
    assert skipped == ["FUD"]


def test_multi_token_add_single_call(tmp_path):
    path = _seed(tmp_path)
    added, _ = blacklist.add_tokens(path, ["AAA", "BBB", "CCC"])
    assert added == ["AAA", "BBB", "CCC"]
    assert {"AAA", "BBB", "CCC"} <= blacklist.load_blacklist(path)


def test_remove_works(tmp_path):
    path = _seed(tmp_path)
    blacklist.add_tokens(path, ["TEMP"])
    removed = blacklist.remove_tokens(path, ["temp"])
    assert removed == ["TEMP"]
    assert "TEMP" not in blacklist.load_blacklist(path)
    assert {"FUD", "EOD"} <= blacklist.load_blacklist(path)  # others untouched


def test_remove_absent_is_noop(tmp_path):
    path = _seed(tmp_path)
    assert blacklist.remove_tokens(path, ["NOPE"]) == []


# --- CLI via main() ----------------------------------------------------------


def _run(argv, env_path, monkeypatch, capsys):
    monkeypatch.setenv("BIZ_DAEMON_BLACKLIST", str(env_path))
    rc = main(argv)
    out = capsys.readouterr().out.strip().splitlines()[-1]
    return rc, json.loads(out)


def test_cli_add_then_list(tmp_path, monkeypatch, capsys):
    path = _seed(tmp_path)
    rc, payload = _run(["blacklist", "add", "rugpull", "ngmi"], path, monkeypatch, capsys)
    assert rc == 0
    assert payload["added"] == ["RUGPULL", "NGMI"]

    rc, payload = _run(["blacklist", "list"], path, monkeypatch, capsys)
    assert rc == 0
    assert {"RUGPULL", "NGMI", "FUD", "EOD"} <= set(payload["denylist"])
    assert payload["denylist_size"] == len(payload["denylist"])


def test_cli_remove(tmp_path, monkeypatch, capsys):
    path = _seed(tmp_path)
    rc, payload = _run(["blacklist", "remove", "FUD"], path, monkeypatch, capsys)
    assert rc == 0
    assert payload["removed"] == ["FUD"]
    assert "FUD" not in blacklist.load_blacklist(path)
