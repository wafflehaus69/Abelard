"""fetch_sec_filing behaviour — atom parsing, body fetch, pagination, errors."""

from __future__ import annotations

import pytest
import requests_mock

from research_daemon.config import Config
from research_daemon.fetch_sec_filing import EDGAR_BROWSE_URL, fetch_sec_filing
from research_daemon.http_client import HttpClient


# ---------- fixtures ----------


_ATOM_TEMPLATE = """<?xml version="1.0" encoding="ISO-8859-1" ?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <company-info>
    <cik>0000320193</cik>
    <conformed-name>Apple Inc.</conformed-name>
  </company-info>
  {entries}
</feed>
"""


def _atom_entry(
    *,
    accession="0000320193-25-000079",
    filing_date="2025-10-31",
    filing_type="10-K",
    filing_href=(
        "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/"
        "0000320193-25-000079-index.htm"
    ),
    form_name="Annual report [Section 13 and 15(d), not S-K Item 405]",
):
    fields = []
    if accession is not None:
        fields.append(f"<accession-number>{accession}</accession-number>")
    if filing_date is not None:
        fields.append(f"<filing-date>{filing_date}</filing-date>")
    if filing_type is not None:
        fields.append(f"<filing-type>{filing_type}</filing-type>")
    if filing_href is not None:
        fields.append(f"<filing-href>{filing_href}</filing-href>")
    if form_name is not None:
        fields.append(f"<form-name>{form_name}</form-name>")
    return f"<entry><content type=\"text/xml\">{''.join(fields)}</content></entry>"


def _atom(entries: list[str]) -> str:
    return _ATOM_TEMPLATE.format(entries="\n  ".join(entries))


def _index_json(docs: list[dict]) -> dict:
    return {
        "directory": {
            "name": "/Archives/edgar/data/320193/000032019325000079",
            "item": docs,
        }
    }


_SAMPLE_HTML = """<html><head><title>Apple Inc. 10-K</title>
<style>body {font-family: sans-serif}</style>
<script>console.log('no');</script>
</head>
<body>
<p>PART I</p>
<p>Item 1. Business</p>
<p>Apple designs and sells smartphones, personal computers, tablets, wearables and accessories.</p>
<p>Item 1A. Risk Factors</p>
<p>The Company's business is subject to risks including global economic conditions.</p>
<p>Item 7. Management's Discussion and Analysis of Financial Condition and Results of Operations</p>
<p>Fiscal 2025 total net sales were record highs.</p>
</body></html>"""


_FOLDER_URL = (
    "https://www.sec.gov/Archives/edgar/data/320193/000032019325000079"
)
_INDEX_URL = f"{_FOLDER_URL}/index.json"
_PRIMARY_DOC_NAME = "aapl-20250927.htm"
_PRIMARY_DOC_URL = f"{_FOLDER_URL}/{_PRIMARY_DOC_NAME}"


# ---------- happy path: metadata only ----------


def test_metadata_only_ok(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([_atom_entry()]))
        env = fetch_sec_filing("AAPL", "10-K", limit=3, config=cfg, client=client)

    assert env["status"] == "ok"
    assert env["data_completeness"] == "metadata_only"
    assert env["source"] == "edgar"
    assert env["warnings"] == []

    data = env["data"]
    assert data["ticker"] == "AAPL"
    assert data["filing_type"] == "10-K"
    assert data["limit"] == 3
    assert data["include_body"] is False
    assert data["item_count"] == 1
    assert data["cik"] == "0000320193"

    item = data["items"][0]
    assert item["accession_number"] == "0000320193-25-000079"
    assert item["filing_type"] == "10-K"
    assert item["filed_at"] == "2025-10-31T00:00:00Z"
    assert item["index_url"].endswith("-index.htm")
    assert item["body"] is None
    assert item["primary_document_url"] is None
    assert item["body_error"] is None
    assert item["body_truncated"] is False


def test_multiple_entries(cfg: Config, client: HttpClient):
    entries = [
        _atom_entry(accession="0000320193-25-000079", filing_date="2025-10-31"),
        _atom_entry(accession="0000320193-24-000123", filing_date="2024-11-01"),
    ]
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom(entries))
        env = fetch_sec_filing("AAPL", "10-K", config=cfg, client=client)
    assert env["data"]["item_count"] == 2
    dates = [it["filed_at"] for it in env["data"]["items"]]
    assert dates == ["2025-10-31T00:00:00Z", "2024-11-01T00:00:00Z"]


def test_empty_feed_is_complete_with_zero_items(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([]))
        env = fetch_sec_filing("AAPL", "10-K", config=cfg, client=client)
    assert env["status"] == "ok"
    assert env["data_completeness"] == "metadata_only"
    assert env["data"]["item_count"] == 0
    assert env["data"]["items"] == []
    assert env["warnings"] == []


def test_per_item_schema_stable(cfg: Config, client: HttpClient):
    expected = {
        "accession_number", "cik", "filing_type", "form_name",
        "filed_at_unix", "filed_at", "index_url", "primary_document_url",
        "body", "body_offset_chars", "body_returned_chars",
        "body_total_chars", "body_truncated", "body_error",
    }
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([_atom_entry(), _atom_entry()]))
        env = fetch_sec_filing("AAPL", "10-K", config=cfg, client=client)
    for item in env["data"]["items"]:
        assert set(item.keys()) == expected


# ---------- include_body=True ----------


def test_include_body_happy_path(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([_atom_entry()]))
        m.get(_INDEX_URL, json=_index_json([
            {"name": _PRIMARY_DOC_NAME, "type": "10-K"},
            {"name": "Financial_Report.xlsx", "type": "EX-101.INS"},
        ]))
        m.get(_PRIMARY_DOC_URL, text=_SAMPLE_HTML)

        env = fetch_sec_filing(
            "AAPL", "10-K", include_body=True, config=cfg, client=client,
        )

    assert env["status"] == "ok"
    assert env["data_completeness"] == "complete"
    assert env["warnings"] == []

    item = env["data"]["items"][0]
    assert item["primary_document_url"] == _PRIMARY_DOC_URL
    assert item["body"] is not None
    assert "Apple designs" in item["body"]
    assert "console.log" not in item["body"]  # script stripped
    assert "sans-serif" not in item["body"]   # style stripped
    assert item["body_total_chars"] is not None
    assert item["body_returned_chars"] == len(item["body"])
    assert item["body_truncated"] is False
    assert item["body_error"] is None


def test_body_truncation_and_offset(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([_atom_entry()]))
        m.get(_INDEX_URL, json=_index_json([
            {"name": _PRIMARY_DOC_NAME, "type": "10-K"},
        ]))
        m.get(_PRIMARY_DOC_URL, text=_SAMPLE_HTML)

        env_full = fetch_sec_filing(
            "AAPL", "10-K", include_body=True,
            max_body_chars=500_000, offset_chars=0,
            config=cfg, client=client,
        )
        env_short = fetch_sec_filing(
            "AAPL", "10-K", include_body=True,
            max_body_chars=20, offset_chars=0,
            config=cfg, client=client,
        )
        env_offset = fetch_sec_filing(
            "AAPL", "10-K", include_body=True,
            max_body_chars=50, offset_chars=30,
            config=cfg, client=client,
        )

    full_body = env_full["data"]["items"][0]["body"]
    short_body = env_short["data"]["items"][0]["body"]
    offset_body = env_offset["data"]["items"][0]["body"]

    # Short: first 20 chars of full; truncated flag set.
    assert short_body == full_body[:20]
    assert env_short["data"]["items"][0]["body_truncated"] is True
    assert env_short["data"]["items"][0]["body_returned_chars"] == 20
    # Offset: starts 30 in, length up to 50.
    assert offset_body == full_body[30:80]
    assert env_offset["data"]["items"][0]["body_offset_chars"] == 30


def test_body_fetch_index_404_sets_per_item_error(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([_atom_entry()]))
        m.get(_INDEX_URL, status_code=404)
        env = fetch_sec_filing(
            "AAPL", "10-K", include_body=True, config=cfg, client=client,
        )

    assert env["status"] == "ok"  # metadata still valid
    assert env["data_completeness"] == "partial"
    item = env["data"]["items"][0]
    assert item["body"] is None
    assert item["body_error"]["reason"] == "not_found"
    # Envelope-level aggregate warning for body failures.
    body_warnings = [w for w in env["warnings"] if w["field"] == "items.body"]
    assert len(body_warnings) == 1
    assert body_warnings[0]["reason"] == "upstream_error"


def test_body_fetch_primary_doc_429(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([_atom_entry()]))
        m.get(_INDEX_URL, json=_index_json([
            {"name": _PRIMARY_DOC_NAME, "type": "10-K"},
        ]))
        m.get(_PRIMARY_DOC_URL, status_code=429)

        env = fetch_sec_filing(
            "AAPL", "10-K", include_body=True, config=cfg, client=client,
        )

    item = env["data"]["items"][0]
    assert item["body"] is None
    assert item["body_error"]["reason"] == "rate_limited"
    assert item["primary_document_url"] == _PRIMARY_DOC_URL
    assert env["data_completeness"] == "partial"


def test_no_matching_primary_document_fallback(cfg: Config, client: HttpClient):
    """When no doc has type==filing_type, fall back to first non-index .htm."""
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([_atom_entry()]))
        m.get(_INDEX_URL, json=_index_json([
            {"name": "something.htm", "type": "EX-99"},
            {"name": "0000320193-25-000079-index.htm", "type": ""},
        ]))
        m.get(f"{_FOLDER_URL}/something.htm", text=_SAMPLE_HTML)

        env = fetch_sec_filing(
            "AAPL", "10-K", include_body=True, config=cfg, client=client,
        )

    item = env["data"]["items"][0]
    assert item["primary_document_url"].endswith("/something.htm")
    assert item["body"] is not None
    assert item["body_error"] is None


def test_index_json_no_usable_document(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([_atom_entry()]))
        m.get(_INDEX_URL, json=_index_json([
            {"name": "data.xml", "type": "EX-101.INS"},
        ]))
        env = fetch_sec_filing(
            "AAPL", "10-K", include_body=True, config=cfg, client=client,
        )
    item = env["data"]["items"][0]
    assert item["body"] is None
    assert item["body_error"]["reason"] == "parse_error"


def test_mixed_success_and_failure_gives_partial(cfg: Config, client: HttpClient):
    entries = [
        _atom_entry(accession="GOOD-001", filing_date="2025-10-31"),
        _atom_entry(
            accession="BAD-002", filing_date="2024-11-01",
            filing_href=(
                "https://www.sec.gov/Archives/edgar/data/320193/"
                "000000000000000000/0000000000-00-000000-index.htm"
            ),
        ),
    ]
    bad_index_url = (
        "https://www.sec.gov/Archives/edgar/data/320193/"
        "000000000000000000/index.json"
    )
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom(entries))
        # GOOD succeeds
        m.get(_INDEX_URL, json=_index_json([
            {"name": _PRIMARY_DOC_NAME, "type": "10-K"},
        ]))
        m.get(_PRIMARY_DOC_URL, text=_SAMPLE_HTML)
        # BAD's index 500s
        m.get(bad_index_url, status_code=500)

        env = fetch_sec_filing(
            "AAPL", "10-K", include_body=True, config=cfg, client=client,
        )

    assert env["status"] == "ok"
    assert env["data_completeness"] == "partial"
    assert env["data"]["item_count"] == 2
    assert env["data"]["body_failure_count"] == 1
    assert env["data"]["items"][0]["body"] is not None
    assert env["data"]["items"][1]["body"] is None
    assert env["data"]["items"][1]["body_error"]["reason"] == "upstream_error"


# ---------- drop behaviour ----------


@pytest.mark.parametrize("drop_field", [
    "accession_number", "filing_type", "filing_href", "filing_date",
])
def test_entries_missing_required_fields_dropped(drop_field, cfg: Config, client: HttpClient):
    good = _atom_entry()
    # Build a bad entry by passing None for the field we want missing.
    kwargs = {
        "accession": "BAD-001",
        "filing_date": "2024-11-01",
        "filing_type": "10-K",
        "filing_href": (
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000000000000000000/0000000000-00-000000-index.htm"
        ),
        "form_name": "Annual report",
    }
    field_map = {
        "accession_number": "accession",
        "filing_date": "filing_date",
        "filing_type": "filing_type",
        "filing_href": "filing_href",
    }
    kwargs[field_map[drop_field]] = None
    bad = _atom_entry(**kwargs)

    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([good, bad]))
        env = fetch_sec_filing("AAPL", "10-K", config=cfg, client=client)

    assert env["data"]["item_count"] == 1
    assert env["data"]["dropped_count"] == 1
    assert env["data_completeness"] == "partial"
    parse_warnings = [w for w in env["warnings"] if w["reason"] == "parse_error"]
    assert len(parse_warnings) == 1
    assert parse_warnings[0]["field"] == "items"


def test_invalid_filing_date_drops_entry(cfg: Config, client: HttpClient):
    bad = _atom_entry(filing_date="not-a-date")
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([bad]))
        env = fetch_sec_filing("AAPL", "10-K", config=cfg, client=client)
    assert env["data"]["item_count"] == 0
    assert env["data"]["dropped_count"] == 1


# ---------- upstream failures ----------


def test_malformed_xml_is_error(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text="<not valid xml><<<")
        env = fetch_sec_filing("AAPL", "10-K", config=cfg, client=client)
    assert env["status"] == "error"
    assert env["data_completeness"] == "none"
    assert "atom" in env["error_detail"].lower() or "parse" in env["error_detail"].lower()


def test_404_is_not_found(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, status_code=404)
        env = fetch_sec_filing("AAPL", "10-K", config=cfg, client=client)
    assert env["status"] == "not_found"


def test_429_is_rate_limited(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, status_code=429)
        env = fetch_sec_filing("AAPL", "10-K", config=cfg, client=client)
    assert env["status"] == "rate_limited"


def test_500_is_error(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, status_code=500)
        env = fetch_sec_filing("AAPL", "10-K", config=cfg, client=client)
    assert env["status"] == "error"


# ---------- User-Agent header ----------


def test_edgar_request_sends_configured_user_agent(cfg: Config, client: HttpClient):
    with requests_mock.Mocker() as m:
        m.get(EDGAR_BROWSE_URL, text=_atom([]))
        fetch_sec_filing("AAPL", "10-K", config=cfg, client=client)
        assert m.last_request.headers["User-Agent"] == client.user_agent


# ---------- input validation ----------


def test_empty_ticker_rejected(cfg: Config, client: HttpClient):
    env = fetch_sec_filing("  ", "10-K", config=cfg, client=client)
    assert env["status"] == "error"


def test_empty_filing_type_rejected(cfg: Config, client: HttpClient):
    env = fetch_sec_filing("AAPL", "  ", config=cfg, client=client)
    assert env["status"] == "error"


@pytest.mark.parametrize("bad", [0, -1, 41, 1000])
def test_limit_out_of_range(bad, cfg: Config, client: HttpClient):
    env = fetch_sec_filing("AAPL", "10-K", limit=bad, config=cfg, client=client)
    assert env["status"] == "error"
    assert "limit" in env["error_detail"]


@pytest.mark.parametrize("bad", [0, -1, 500_001])
def test_max_body_chars_out_of_range(bad, cfg: Config, client: HttpClient):
    env = fetch_sec_filing(
        "AAPL", "10-K", max_body_chars=bad, config=cfg, client=client,
    )
    assert env["status"] == "error"


def test_negative_offset_rejected(cfg: Config, client: HttpClient):
    env = fetch_sec_filing(
        "AAPL", "10-K", offset_chars=-1, config=cfg, client=client,
    )
    assert env["status"] == "error"


@pytest.mark.parametrize("bad", ["3", 3.0, None, True])
def test_non_int_limit_rejected(bad, cfg: Config, client: HttpClient):
    env = fetch_sec_filing("AAPL", "10-K", limit=bad, config=cfg, client=client)  # type: ignore[arg-type]
    assert env["status"] == "error"


def test_non_bool_include_body_rejected(cfg: Config, client: HttpClient):
    env = fetch_sec_filing(
        "AAPL", "10-K", include_body="yes", config=cfg, client=client,  # type: ignore[arg-type]
    )
    assert env["status"] == "error"
