"""tableview: pure-presentation rendering + CLI --table flag."""

from __future__ import annotations

import json

import pytest

from biz_daemon import cli
from biz_daemon.cli import main
from biz_daemon.tableview import ATTN_FALSE, ATTN_TRUE, EM_DASH, render_table

PAYLOAD = {
    "scrape_ts": 1_717_430_400,
    "threads": [{"no": 100, "subject": "/smg/ - Stock Market General", "post_count": 312}],
    "tickers": [
        {
            "ticker": "GME",
            "mentions": 14,
            "attention": True,
            "sentiment": {
                "directional": 8,
                "neutral": 2,
                "pct_bullish": 25,
                "pct_bearish": 75,
                "read": "bearish",
            },
            "sample_post_ids": [1, 2],
        },
        {
            "ticker": "NTR",
            "mentions": 2,
            "attention": False,
            "sentiment": None,
            "sample_post_ids": [9],
        },
    ],
    "cost": {"haiku_calls": 2, "input_tokens": 1_000_000, "output_tokens": 200_000},
    "errors": [],
}

ERROR_PAYLOAD = {
    "scrape_ts": 1_717_430_400,
    "threads": [],
    "tickers": [],
    "cost": {"haiku_calls": 0, "input_tokens": 0, "output_tokens": 0},
    "errors": ["fourchan: no live /smg/ thread found"],
}


# --- pure renderer -----------------------------------------------------------


def test_header_has_counts_and_columns():
    out = render_table(PAYLOAD)
    assert "threads: 1" in out
    assert "posts: 312" in out
    assert "TICKER" in out and "MENTIONS" in out and "ATTN" in out and "SENTIMENT" in out


def test_attention_row_shows_sentiment_and_marker():
    out = render_table(PAYLOAD)
    gme_line = next(ln for ln in out.splitlines() if ln.startswith("GME"))
    assert ATTN_TRUE in gme_line
    assert "bearish 25/75 (8 dir, 2 neu)" in gme_line


def test_tail_row_shows_placeholder_and_dot():
    out = render_table(PAYLOAD)
    ntr_line = next(ln for ln in out.splitlines() if ln.startswith("NTR"))
    assert ATTN_FALSE in ntr_line
    assert EM_DASH in ntr_line
    assert "neu" not in ntr_line  # no sentiment detail on the tail


def test_separator_between_attention_and_tail():
    out = render_table(PAYLOAD)
    # a dotted separator line appears before the first tail row
    assert any(set(ln) == {ATTN_FALSE} for ln in out.splitlines())


def test_footer_has_cost_and_error_count():
    out = render_table(PAYLOAD)
    # 1M input @ $1 + 0.2M output @ $5 = $2.0000
    assert "haiku_calls: 2" in out
    assert "$2.0000" in out
    assert "errors: 0" in out


def test_error_state_prints_loudly():
    out = render_table(ERROR_PAYLOAD)
    assert "ERRORS:" in out
    assert "no live /smg/ thread found" in out
    assert "(no tickers)" in out  # not a silently blank table


# --- CLI wiring --------------------------------------------------------------


@pytest.fixture
def _patched(monkeypatch):
    monkeypatch.setenv("FINNHUB_API_KEY", "k")
    monkeypatch.setattr(cli, "run_scrape", lambda cfg: PAYLOAD)


def test_default_is_json(_patched, capsys):
    rc = main([])
    out = capsys.readouterr().out.strip()
    parsed = json.loads(out)  # must still be valid JSON
    assert parsed["tickers"][0]["ticker"] == "GME"
    assert rc == 0


def test_json_flag_is_json(_patched, capsys):
    main(["--json"])
    json.loads(capsys.readouterr().out.strip())  # valid JSON


def test_table_flag_renders_table_not_json(_patched, capsys):
    rc = main(["--table"])
    out = capsys.readouterr().out
    assert "TICKER" in out and "bearish 25/75" in out
    with pytest.raises(json.JSONDecodeError):
        json.loads(out.strip())  # table is NOT json
    assert rc == 0


def test_table_does_not_mutate_payload(_patched, capsys):
    before = json.dumps(PAYLOAD, sort_keys=True)
    main(["--table"])
    capsys.readouterr()
    assert json.dumps(PAYLOAD, sort_keys=True) == before  # pure presentation


def test_table_error_state_prints_loudly(monkeypatch, capsys):
    monkeypatch.setenv("FINNHUB_API_KEY", "k")
    monkeypatch.setattr(cli, "run_scrape", lambda cfg: ERROR_PAYLOAD)
    rc = main(["--table"])
    out = capsys.readouterr().out
    assert "ERRORS:" in out and "no live /smg/ thread found" in out
    assert rc == 1  # error state still exits non-zero
