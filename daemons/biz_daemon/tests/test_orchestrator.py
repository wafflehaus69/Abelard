"""orchestrator: contract assembly, attention flags, persistence, no-thread."""

from __future__ import annotations

import dataclasses
import json
from types import SimpleNamespace

import pytest

from abelard_common import fourchan_fetch as fourchan_client
from biz_daemon import storage
from biz_daemon.orchestrator import run_scrape

SCRAPE_TS = 1_717_430_400


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)

    def get(self, url, headers=None, timeout=None):
        return self._responses.pop(0)


class FakeAnthropic:
    def __init__(self, classifications):
        self._classifications = classifications
        self.calls = 0
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        self.calls += 1
        block = SimpleNamespace(
            type="text",
            text=json.dumps({"classifications": self._classifications}),
        )
        usage = SimpleNamespace(
            input_tokens=100,
            output_tokens=20,
            cache_read_input_tokens=0,
            cache_creation_input_tokens=0,
        )
        return SimpleNamespace(content=[block], usage=usage)


def _fetcher(responses):
    return fourchan_client.Fetcher(
        user_agent="t",
        session=FakeSession(responses),
        sleep=lambda _s: None,
        clock=lambda: 0.0,
    )


_CATALOG = [{"page": 1, "threads": [{"no": 100, "sub": "/smg/ - Stock Market General"}]}]
_THREAD = {"posts": [
    {"no": 100, "sub": "/smg/ - Stock Market General", "com": "GME bull"},
    {"no": 101, "com": "GME up"},
    {"no": 102, "com": "GME"},
    {"no": 103, "com": "GME moon"},
    {"no": 104, "com": "GME"},
    {"no": 105, "com": "AMD"},
    {"no": 106, "com": "AMD NTR"},
    {"no": 107},  # image-only
]}


def _seed_universe(conn):
    storage.write_cached_universe(
        conn, symbols={"GME", "AMD", "NTR"}, source="finnhub", now=SCRAPE_TS
    )


def test_full_tail_returned_with_attention_flags(cfg, conn):
    _seed_universe(conn)
    client = FakeAnthropic(
        [{"post_id": n, "ticker": "GME", "stance": "bullish"} for n in (100, 101, 102, 103, 104)]
    )
    payload = run_scrape(
        cfg,
        now=SCRAPE_TS,
        fetcher=_fetcher([FakeResponse(200, _CATALOG), FakeResponse(200, _THREAD)]),
        conn=conn,
        anthropic_client=client,
    )

    assert payload["scrape_ts"] == SCRAPE_TS
    assert payload["threads"] == [
        {"no": 100, "subject": "/smg/ - Stock Market General", "post_count": 8}
    ]
    by_ticker = {t["ticker"]: t for t in payload["tickers"]}
    # full tail present
    assert set(by_ticker) == {"GME", "AMD", "NTR"}
    # ranking: GME(5) > AMD(2) > NTR(1)
    assert [t["ticker"] for t in payload["tickers"]] == ["GME", "AMD", "NTR"]

    assert by_ticker["GME"]["attention"] is True
    assert by_ticker["GME"]["mentions"] == 5
    assert by_ticker["GME"]["sentiment"]["read"] == "bullish"

    # tail tickers: attention false, sentiment null
    assert by_ticker["AMD"]["attention"] is False
    assert by_ticker["AMD"]["sentiment"] is None
    assert by_ticker["NTR"]["attention"] is False
    assert by_ticker["NTR"]["sentiment"] is None

    assert payload["cost"]["haiku_calls"] == 1
    assert payload["errors"] == []
    assert client.calls == 1


def test_snapshot_persisted_with_full_cost(cfg, conn):
    _seed_universe(conn)
    client = FakeAnthropic(
        [{"post_id": n, "ticker": "GME", "stance": "bullish"} for n in (100, 101, 102, 103, 104)]
    )
    run_scrape(
        cfg,
        now=SCRAPE_TS,
        fetcher=_fetcher([FakeResponse(200, _CATALOG), FakeResponse(200, _THREAD)]),
        conn=conn,
        anthropic_client=client,
    )
    row = conn.execute(
        "SELECT scrape_ts, payload_json, cost_json, created_at FROM snapshots WHERE scrape_ts = ?",
        (SCRAPE_TS,),
    ).fetchone()
    assert row is not None
    assert row["scrape_ts"] == SCRAPE_TS
    assert row["created_at"] == SCRAPE_TS  # single canonical timestamp threaded
    # full cost record (incl cache fields) preserved in storage, not just the 3-key public view
    cost = json.loads(row["cost_json"])
    assert cost["haiku_calls"] == 1
    assert "cache_read_input_tokens" in cost
    persisted = json.loads(row["payload_json"])
    assert persisted["scrape_ts"] == SCRAPE_TS


def test_no_smg_thread_state_propagates(cfg, conn):
    empty_catalog = [{"page": 1, "threads": [{"no": 1, "sub": "biz general"}]}]
    payload = run_scrape(
        cfg,
        now=SCRAPE_TS,
        fetcher=_fetcher([FakeResponse(200, empty_catalog)]),
        conn=conn,
    )
    assert payload["threads"] == []
    assert payload["tickers"] == []
    assert payload["errors"] == ["fourchan: no live /smg/ thread found"]
    # still persisted as substrate, not a silent drop
    row = conn.execute(
        "SELECT scrape_ts FROM snapshots WHERE scrape_ts = ?", (SCRAPE_TS,)
    ).fetchone()
    assert row is not None


_THREAD_FLOOR = {"posts": [
    {"no": 100, "sub": "/smg/ - Stock Market General", "com": "GME bull"},
    {"no": 101, "com": "GME"},
    {"no": 102, "com": "GME"},
    {"no": 103, "com": "GME"},
    {"no": 104, "com": "GME"},   # GME x5 -> attention + sentiment
    {"no": 105, "com": "AMD"},
    {"no": 106, "com": "AMD"},
    {"no": 107, "com": "AMD"},   # AMD x3 -> sentiment, NOT attention
    {"no": 108, "com": "NTR"},
    {"no": 109, "com": "NTR"},   # NTR x2 -> below floor, no sentiment
]}


def test_sentiment_floor_decoupled_from_attention(cfg, conn):
    _seed_universe(conn)
    classifications = (
        [{"post_id": n, "ticker": "GME", "stance": "bullish"} for n in (100, 101, 102, 103, 104)]
        + [{"post_id": n, "ticker": "AMD", "stance": "bearish"} for n in (105, 106, 107)]
    )
    client = FakeAnthropic(classifications)
    payload = run_scrape(
        cfg,
        now=SCRAPE_TS,
        fetcher=_fetcher([FakeResponse(200, _CATALOG), FakeResponse(200, _THREAD_FLOOR)]),
        conn=conn,
        anthropic_client=client,
    )
    by_ticker = {t["ticker"]: t for t in payload["tickers"]}

    # GME: 5 mentions -> attention ● AND sentiment
    assert by_ticker["GME"]["attention"] is True
    assert by_ticker["GME"]["sentiment"]["read"] == "bullish"

    # AMD: 3 mentions -> sentiment present, attention flag still false
    assert by_ticker["AMD"]["attention"] is False
    assert by_ticker["AMD"]["sentiment"] is not None
    assert by_ticker["AMD"]["sentiment"]["read"] == "bearish"

    # NTR: 2 mentions -> below floor, count-only, sentiment null
    assert by_ticker["NTR"]["attention"] is False
    assert by_ticker["NTR"]["sentiment"] is None


def test_missing_anthropic_key_yields_structured_error_not_fabrication(cfg, conn):
    _seed_universe(conn)
    cfg_no_key = dataclasses.replace(cfg, anthropic_api_key=None)
    payload = run_scrape(
        cfg_no_key,
        now=SCRAPE_TS,
        fetcher=_fetcher([FakeResponse(200, _CATALOG), FakeResponse(200, _THREAD)]),
        conn=conn,
    )
    gme = next(t for t in payload["tickers"] if t["ticker"] == "GME")
    assert gme["attention"] is True
    assert "error" in gme["sentiment"]
    assert any("sentiment" in e for e in payload["errors"])
