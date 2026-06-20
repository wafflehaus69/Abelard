"""CLI — scan (aggregate+persist+emit) and read-chatter; fail-loud + exit semantics.

Every scan writes the baseline DB + archive to a per-test tmp dir (autouse), so the
suite stays hermetic and never touches the daemon's real state/ or archive/.
"""

from __future__ import annotations

import json

import pytest

from chatter_daemon.cli import main
from chatter_daemon.sources.finnhub_news import FinnhubNewsSource


@pytest.fixture(autouse=True)
def _isolate_state(tmp_path, monkeypatch):
    monkeypatch.setenv("CHATTER_BASELINE_DB", str(tmp_path / "baseline.sqlite3"))
    monkeypatch.setenv("CHATTER_ARCHIVE_ROOT", str(tmp_path / "archive"))


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


def test_scan_watchlist_loads(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    _no_sources(monkeypatch)  # spine-only: no live source
    rc, payload = _run(["scan", "--watchlist", "alpha"], capsys)
    assert rc == 0
    assert payload["errors"] == []
    assert payload["canonical_ts"] is not None
    assert payload["scan_id"].startswith("cd-")
    assert {w["label"] for w in payload["windows"]} == {"24h", "7d", "monthly"}
    assert payload["watchlists"][0]["name"] == "alpha"
    assert payload["tickers"] == []  # no sources -> no records -> no aggregated tickers
    assert payload["sources"] == []
    assert payload["degraded"] is False


def test_scan_all_enumerates(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    _no_sources(monkeypatch)
    rc, payload = _run(["scan", "--all"], capsys)
    assert rc == 0
    assert [w["name"] for w in payload["watchlists"]] == ["alpha", "beta"]


def test_scan_missing_list_fails_loud(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    rc, payload = _run(["scan", "--watchlist", "nope"], capsys)
    assert rc == 1  # spine failure (bad watchlist) -> exit 1
    assert payload["errors"][0].startswith("watchlist: watchlist not found")


def test_requires_subcommand(capsys):
    with pytest.raises(SystemExit):
        main([])


def test_scan_requires_a_target(capsys):
    with pytest.raises(SystemExit):
        main(["scan"])


def test_scan_bundled_barber_growth_validates(monkeypatch, capsys):
    # Hermetic: loads the REAL bundled watchlist; no live source.
    _no_sources(monkeypatch)
    rc, payload = _run(["scan", "--watchlist", "barber_growth"], capsys)
    assert rc == 0
    assert payload["watchlists"][0]["name"] == "barber_growth"
    assert payload["watchlists"][0]["tickers"] == 46
    assert payload["watchlists"][0]["active"] == 45  # P excluded (enabled=false)


def test_scan_finnhub_aggregates_exit0(watchlists_dir, monkeypatch, capsys):
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
    rc, payload = _run(["scan", "--watchlist", "alpha"], capsys)
    assert rc == 0
    assert payload["degraded"] is False
    assert payload["sources"][0]["source"] == "finnhub_news"
    assert payload["sources"][0]["ok"] is True
    nvda = next(t for t in payload["tickers"] if t["ticker"] == "NVDA")
    fin = next(s for s in nvda["sources"] if s["source"] == "finnhub_news")
    assert fin["metrics"]["mention_count"] == 1
    assert fin["sentiment"]["method"] == "none"
    assert fin["anomaly"]["state"] == "building"  # first run -> no baseline yet


def test_scan_total_source_failure_no_key_exit1(watchlists_dir, monkeypatch, capsys):
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    monkeypatch.setattr(
        "chatter_daemon.cli.build_sources",
        lambda cfg: [FinnhubNewsSource(api_key=None)],  # no key -> raises -> total failure
    )
    rc, payload = _run(["scan", "--watchlist", "alpha"], capsys)
    assert rc == 1  # total source failure (zero records, every source errored)
    assert payload["tickers"] == []
    assert payload["sources"][0]["ok"] is False
    assert payload["degraded"] is True
    assert any("FINNHUB_API_KEY" in e for e in payload["errors"])


def test_read_chatter_renders(watchlists_dir, monkeypatch, capsys, tmp_path):
    # Run a scan to persist an artifact, then render it back.
    monkeypatch.setenv("CHATTER_WATCHLISTS_DIR", str(watchlists_dir))
    client = _FakeClient([[{"headline": "NVDA pops", "url": "http://a"}], []])
    monkeypatch.setattr(
        "chatter_daemon.cli.build_sources",
        lambda cfg: [FinnhubNewsSource(api_key="k", client=client)],
    )
    rc, payload = _run(["scan", "--watchlist", "alpha"], capsys)
    assert rc == 0
    scan_id = payload["scan_id"]
    files = list((tmp_path / "archive").rglob(f"{scan_id}.json"))
    assert len(files) == 1  # persisted under YYYY-MM partition

    rc2 = main(["read-chatter", str(files[0])])
    out = capsys.readouterr().out
    assert rc2 == 0
    assert f"chatter scan {scan_id}" in out
    assert "NVDA" in out
    assert "headlines" in out  # Finnhub source-labeled count


def test_read_chatter_missing_path_fails_loud(capsys, tmp_path):
    rc = main(["read-chatter", str(tmp_path / "nope.json")])
    assert rc == 1
    err = capsys.readouterr().err
    assert "read-chatter error" in err and "does not exist" in err


def test_attention_real_scan_persists_and_reads(monkeypatch, capsys, tmp_path):
    import chatter_daemon.cli as C
    from chatter_daemon.discovery import SurfaceCounts
    from chatter_daemon.ticker_universe import UniverseResult

    monkeypatch.setattr(
        C.ticker_universe, "load_universe",
        lambda *a, **k: UniverseResult(frozenset({"GME", "NVDA"}), "finnhub"),
    )
    monkeypatch.setattr(
        C, "run_dry_run",
        lambda **k: [SurfaceCounts("smg_freq", "24h /smg/ posts", {"GME": 7, "NVDA": 2})],
    )
    rc, payload = _run(["attention"], capsys)  # real scan (no --dry-run)
    assert rc == 0
    assert payload["scan_mode"] == "attention"
    admitted = {t["ticker"] for t in payload["tickers"]}
    assert "GME" in admitted and "NVDA" not in admitted  # NVDA 2 < /smg/ floor 3

    scan_id = payload["scan_id"]
    files = list((tmp_path / "archive").rglob(f"{scan_id}.json"))
    assert len(files) == 1  # persisted under YYYY-MM
    rc2 = main(["read-chatter", str(files[0])])
    out = capsys.readouterr().out
    assert rc2 == 0 and "ATTENTION scan" in out and "SALIENCE" in out and "GME" in out


def test_attention_dry_run_prints_distribution(monkeypatch, capsys):
    import chatter_daemon.cli as C
    from chatter_daemon.discovery import SurfaceCounts
    from chatter_daemon.ticker_universe import UniverseResult

    # Mock the universe load + the pull so no network/transport runs.
    monkeypatch.setattr(
        C.ticker_universe, "load_universe",
        lambda *a, **k: UniverseResult(frozenset({"GME"}), "finnhub"),
    )
    monkeypatch.setattr(
        C, "run_dry_run",
        lambda **k: [SurfaceCounts("smg_freq", "24h /smg/ posts", {"GME": 7})],
    )
    rc = main(["attention", "--dry-run"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "ATTENTION calibration" in out and "smg_freq" in out and "GME" in out


def test_attention_universe_unavailable_exit1(monkeypatch, capsys):
    import chatter_daemon.cli as C
    from chatter_daemon.ticker_universe import UniverseError

    def boom(*a, **k):
        raise UniverseError("no live endpoint and no fallback")

    monkeypatch.setattr(C.ticker_universe, "load_universe", boom)
    rc = main(["attention", "--dry-run"])
    assert rc == 1
    assert "universe unavailable" in capsys.readouterr().err
