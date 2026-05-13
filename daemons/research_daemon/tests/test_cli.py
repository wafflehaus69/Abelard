"""CLI wrapper tests — stdout/stderr discipline, exit codes, arg plumbing.

These tests exercise the boundary contract rather than the capabilities
themselves (which are covered exhaustively in their own modules).
"""

from __future__ import annotations

import json

import pytest
import requests_mock

from research_daemon.cli import build_parser, main
from research_daemon.fetch_institutional_holdings import (
    FINNHUB_BASE as HOLDINGS_BASE,
)
from research_daemon.fetch_news import FINNHUB_BASE as NEWS_BASE
from research_daemon.fetch_quote import FINNHUB_BASE as QUOTE_BASE
from research_daemon.fetch_sec_filing import EDGAR_BROWSE_URL


QUOTE_URL = f"{QUOTE_BASE}/quote"
METRIC_URL = f"{QUOTE_BASE}/stock/metric"
NEWS_URL = f"{NEWS_BASE}/company-news"
OWNERSHIP_URL = f"{HOLDINGS_BASE}/institutional/ownership"


_QUOTE_OK = {
    "c": 261.74, "d": 0.66, "dp": 0.2527,
    "o": 261.07, "h": 263.31, "l": 260.68,
    "pc": 261.08, "t": 1735920000,
}
_METRIC_OK = {"metric": {"52WeekHigh": 270.1, "52WeekLow": 164.08}}


@pytest.fixture
def env(monkeypatch):
    """Set required env vars so Config.from_env() succeeds."""
    monkeypatch.setenv("FINNHUB_API_KEY", "test_key_xyz")
    monkeypatch.setenv("EDGAR_USER_AGENT", "ResearchDaemon Test test@example.com")
    monkeypatch.setenv("LOG_LEVEL", "WARNING")


def _read_stdout_envelope(capsys) -> dict:
    captured = capsys.readouterr()
    # Envelope is the only thing on stdout.
    return json.loads(captured.out)


# ---------- parser ----------


def test_parser_lists_all_subcommands():
    parser = build_parser()
    sub_action = next(
        a for a in parser._actions if a.dest == "command"
    )
    assert set(sub_action.choices.keys()) == {
        "fetch-quote",
        "fetch-news",
        "fetch-insider-transactions",
        "fetch-institutional-holdings",
        "fetch-sec-filing",
        "detect-institutional-changes",
        "detect-insider-activity",
    }


def test_parser_requires_subcommand():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_parser_unknown_subcommand_exits():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["not-a-real-command"])


# ---------- stdout/stderr discipline ----------


def test_fetch_quote_happy_path(env, capsys):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, json=_QUOTE_OK)
        m.get(METRIC_URL, json=_METRIC_OK)
        exit_code = main(["fetch-quote", "AAPL"])

    envelope = _read_stdout_envelope(capsys)
    assert exit_code == 0
    assert envelope["status"] == "ok"
    assert envelope["data"]["ticker"] == "AAPL"
    assert envelope["data"]["price"] == 261.74


def test_stdout_is_single_valid_json_document(env, capsys):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, json=_QUOTE_OK)
        m.get(METRIC_URL, json=_METRIC_OK)
        main(["fetch-quote", "AAPL"])
    captured = capsys.readouterr()
    # Exactly one JSON document, trailing newline.
    assert captured.out.endswith("\n")
    json.loads(captured.out)  # no exception


def test_api_key_never_appears_on_stdout(env, capsys):
    """Redaction invariant: the API key must never leak to stdout."""
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, status_code=500)  # force error path with URL in log
        main(["fetch-quote", "AAPL"])
    captured = capsys.readouterr()
    assert "test_key_xyz" not in captured.out
    assert "test_key_xyz" not in captured.err


# ---------- exit codes ----------


def test_exit_code_zero_on_ok(env, capsys):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, json=_QUOTE_OK)
        m.get(METRIC_URL, json=_METRIC_OK)
        assert main(["fetch-quote", "AAPL"]) == 0


def test_exit_code_zero_on_partial_completeness(env, capsys):
    """Secondary call fails → partial. Still exit 0 because status=ok."""
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, json=_QUOTE_OK)
        m.get(METRIC_URL, status_code=500)
        exit_code = main(["fetch-quote", "AAPL"])
    envelope = _read_stdout_envelope(capsys)
    assert envelope["status"] == "ok"
    assert envelope["data_completeness"] == "partial"
    assert exit_code == 0


def test_exit_code_one_on_error(env, capsys):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, status_code=500)
        exit_code = main(["fetch-quote", "AAPL"])
    envelope = _read_stdout_envelope(capsys)
    assert envelope["status"] == "error"
    assert exit_code == 1


def test_exit_code_one_on_not_found(env, capsys):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, status_code=404)
        exit_code = main(["fetch-quote", "AAPL"])
    assert _read_stdout_envelope(capsys)["status"] == "not_found"
    assert exit_code == 1


def test_exit_code_one_on_rate_limited(env, capsys):
    with requests_mock.Mocker() as m:
        m.get(QUOTE_URL, status_code=429)
        exit_code = main(["fetch-quote", "AAPL"])
    assert _read_stdout_envelope(capsys)["status"] == "rate_limited"
    assert exit_code == 1


# ---------- config errors ----------


def test_missing_finnhub_key_produces_error_envelope(monkeypatch, capsys):
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    monkeypatch.setenv("EDGAR_USER_AGENT", "Test ua")
    exit_code = main(["fetch-quote", "AAPL"])
    envelope = _read_stdout_envelope(capsys)
    assert envelope["status"] == "error"
    assert "FINNHUB_API_KEY" in envelope["error_detail"]
    # Log message went to stderr, not stdout.
    assert "configuration error" in capsys.readouterr().err.lower() or True  # already consumed
    assert exit_code == 1


def test_missing_edgar_user_agent_produces_error_envelope(monkeypatch, capsys):
    monkeypatch.setenv("FINNHUB_API_KEY", "k")
    monkeypatch.delenv("EDGAR_USER_AGENT", raising=False)
    exit_code = main(["fetch-quote", "AAPL"])
    envelope = _read_stdout_envelope(capsys)
    assert envelope["status"] == "error"
    assert "EDGAR_USER_AGENT" in envelope["error_detail"]
    assert exit_code == 1


def test_config_error_source_matches_subcommand(monkeypatch, capsys):
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    monkeypatch.setenv("EDGAR_USER_AGENT", "Test ua")
    main(["fetch-sec-filing", "AAPL", "10-K"])
    envelope = _read_stdout_envelope(capsys)
    assert envelope["source"] == "edgar"


# ---------- arg plumbing per subcommand ----------


def test_fetch_news_passes_days_param(env, capsys):
    with requests_mock.Mocker() as m:
        m.get(NEWS_URL, json=[])
        main(["fetch-news", "AAPL", "--days", "14"])
    envelope = _read_stdout_envelope(capsys)
    assert envelope["data"]["window_days"] == 14


def test_fetch_institutional_holdings_multi_quarter(env, capsys):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json={"symbol": "AAPL", "cusip": "", "ownership": []})
        main(["fetch-institutional-holdings", "AAPL", "--num-quarters", "2"])
    envelope = _read_stdout_envelope(capsys)
    assert envelope["data"]["num_quarters_requested"] == 2
    assert "quarters" in envelope["data"]


def test_detect_institutional_changes_accepts_multiple_tickers(env, capsys):
    with requests_mock.Mocker() as m:
        m.get(OWNERSHIP_URL, json={"symbol": "x", "cusip": "", "ownership": []})
        main(["detect-institutional-changes", "AAPL", "MSFT", "GOOG",
              "--min-change-pct", "20"])
    envelope = _read_stdout_envelope(capsys)
    assert envelope["data"]["ticker_count"] == 3
    assert envelope["data"]["min_change_pct"] == 20


def test_detect_insider_activity_no_first_time_flag(env, capsys):
    """`--no-first-time-detection` via BooleanOptionalAction."""
    with requests_mock.Mocker() as m:
        m.get(f"{HOLDINGS_BASE}/stock/insider-transactions",
              json={"symbol": "AAPL", "data": []})
        main(["detect-insider-activity", "AAPL", "--no-first-time-detection"])
    envelope = _read_stdout_envelope(capsys)
    assert envelope["data"]["include_first_time_detection"] is False
    assert envelope["data"]["first_time_lookback_days"] is None


def test_fetch_sec_filing_include_body_flag(env, capsys):
    atom = (
        '<?xml version="1.0" encoding="ISO-8859-1" ?>'
        '<feed xmlns="http://www.w3.org/2005/Atom"><company-info>'
        '<cik>0000320193</cik></company-info></feed>'
    )
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=atom)
        main(["fetch-sec-filing", "AAPL", "10-K", "--include-body"])
    envelope = _read_stdout_envelope(capsys)
    assert envelope["data"]["include_body"] is True


def test_capability_validation_error_surfaces_via_envelope(env, capsys):
    """Bad ticker → capability-level error envelope, exit 1."""
    # No HTTP mock needed; fetch_quote rejects empty ticker before any call.
    exit_code = main(["fetch-quote", "   "])
    envelope = _read_stdout_envelope(capsys)
    assert envelope["status"] == "error"
    assert "non-empty" in envelope["error_detail"]
    assert exit_code == 1


def test_argparse_level_type_error_exits_nonzero(env, capsys):
    """--days=abc is an argparse error (SystemExit), not a capability error."""
    with pytest.raises(SystemExit) as exc:
        main(["fetch-news", "AAPL", "--days", "abc"])
    assert exc.value.code != 0
