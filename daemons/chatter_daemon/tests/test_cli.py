"""CLI — load/validate, --all, fail-loud, exit semantics, live-source wiring."""

from __future__ import annotations

import json

import pytest

from chatter_daemon.cli import main
from chatter_daemon.sources.finnhub_news import FinnhubNewsSource


class _FakeClient:
    """Stand-in for HttpClient.get_json — returns queued payloads (or raises)."""

    def __init__(self, payloads):
        self._payloads = list(payloads)

    def get_json(self, url, *, params=None, headers=None, timeout=None):
        p = self._payloads.pop(0)
        if isinstance(p, Exception):
            raise p
        return p


def _run(argv, capsys):
    rc = main(argv)
    out = capsys.readouterr().out.strip().splitlines()[-1]
    return rc, json.loads(out)


def _no_sources(monkeypatch):
    monkeypatch.setattr("chatter_daemon.cli.build_sources", lambda cfg: [])


def test_cli_watchlist_loads(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    _no_sources(monkeypatch)  # spine-only: no live source
    rc, payload = _run(["--watchlist", "alpha"], capsys)
    assert rc == 0
    assert payload["errors"] == []
    assert payload["canonical_ts"] is not None
    assert {w["label"] for w in payload["windows"]} == {"24h", "7d", "monthly"}
    assert payload["watchlists"][0]["name"] == "alpha"
    assert payload["records"] == []
    assert payload["sources"] == []
    assert payload["degraded"] is False


def test_cli_all_enumerates(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    _no_sources(monkeypatch)
    rc, payload = _run(["--all"], capsys)
    assert rc == 0
    assert [w["name"] for w in payload["watchlists"]] == ["alpha", "beta"]


def test_cli_missing_list_fails_loud(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    rc, payload = _run(["--watchlist", "nope"], capsys)
    assert rc == 1  # spine failure (bad watchlist) -> exit 1
    assert payload["errors"][0].startswith("watchlist: watchlist not found")


def test_cli_requires_a_target(capsys):
    with pytest.raises(SystemExit):
        main([])


def test_cli_bundled_barber_growth_validates(monkeypatch, capsys):
    # Hermetic: loads the REAL bundled watchlist; no live source.
    _no_sources(monkeypatch)
    rc, payload = _run(["--watchlist", "barber_growth"], capsys)
    assert rc == 0
    assert payload["watchlists"][0]["name"] == "barber_growth"
    assert payload["watchlists"][0]["tickers"] == 46
    assert payload["watchlists"][0]["active"] == 45  # P excluded (enabled=false)


def test_cli_finnhub_records_exit0(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    # `alpha` has NVDA + AMD; queue one payload per active ticker.
    client = _FakeClient([
        [{"headline": "NVDA pops", "url": "http://a", "datetime": 1}],
        [],
    ])
    monkeypatch.setattr(
        "chatter_daemon.cli.build_sources",
        lambda cfg: [FinnhubNewsSource(api_key="k", client=client)],
    )
    rc, payload = _run(["--watchlist", "alpha"], capsys)
    assert rc == 0
    assert payload["degraded"] is False
    assert payload["sources"][0]["source"] == "finnhub_news"
    assert payload["sources"][0]["ok"] is True
    assert len(payload["records"]) == 2
    nvda = next(r for r in payload["records"] if r["ticker"] == "NVDA")
    assert nvda["metrics"]["mention_count"] == 1
    assert nvda["sentiment"]["method"] == "none"


def test_cli_total_source_failure_no_key_exit1(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    monkeypatch.setattr(
        "chatter_daemon.cli.build_sources",
        lambda cfg: [FinnhubNewsSource(api_key=None)],  # no key -> raises -> total failure
    )
    rc, payload = _run(["--watchlist", "alpha"], capsys)
    assert rc == 1  # total source failure (zero records, every source errored)
    assert payload["records"] == []
    assert payload["sources"][0]["ok"] is False
    assert payload["degraded"] is True
    assert any("FINNHUB_API_KEY" in e for e in payload["errors"])
