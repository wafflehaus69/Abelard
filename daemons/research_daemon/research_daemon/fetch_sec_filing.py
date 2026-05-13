"""fetch_sec_filing — recent filings of a given type (10-K, 10-Q, 8-K, DEF 14A)
for a ticker, from EDGAR.

Primary call: SEC's browse-edgar Atom feed, which accepts a ticker directly
and needs no CIK lookup. Feed gives accession number, filing date, filing
type, and the index URL for each filing. No body by default — just metadata.

When `include_body=True`, each filing triggers two additional requests:
  1. /{folder}/index.json — find the primary document
  2. /{folder}/{primary_doc} — fetch the document itself
Text is extracted via BeautifulSoup, whitespace normalised. Byte-level
pagination via `offset_chars` + `max_body_chars`.

Section-specific extraction (MD&A, risk factors, segments) is NOT
implemented. 10-K HTML is too heterogeneous for reliable anchor detection,
and section boundaries are interpretive (TOC vs. body, bolded inline refs
vs. headings). Abelard's LLM layer is better suited to locate sections
within the returned text. Use `offset_chars` to paginate.

Per-filing schema (stable):

    {
      "accession_number":     str,
      "cik":                  str | null,
      "filing_type":          str,
      "form_name":            str | null,
      "filed_at_unix":        int,
      "filed_at":             str,   # ISO-8601 UTC, midnight of filing date
      "index_url":            str,
      "primary_document_url": str | null,   # populated only when include_body=True
      "body":                 str | null,
      "body_offset_chars":    int,
      "body_returned_chars":  int,          # 0 when body is null
      "body_total_chars":     int | null,   # full extracted text length
      "body_truncated":       bool,
      "body_error":           {"reason": str, "detail": str} | null,
    }

Required atom fields for parsing (drop if missing): accession-number,
filing-date, filing-type, filing-href. Drops produce a `parse_error`
envelope warning and increment `data.dropped_count`.

Per-item body failures don't drop the filing — metadata is still returned
with body=null and a structured `body_error`. Envelope-level warning
aggregates body failures across items.

When `include_body=False`, `data_completeness="metadata_only"` (explicit
signal that bodies were not fetched). When `include_body=True`, complete
if all bodies fetched; partial if any body failed or any item dropped.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup

from .config import Config
from .envelope import Completeness, build_error, build_ok, make_warning
from .http_client import HttpClient, NotFound, RateLimited, TransportError


EDGAR_BROWSE_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}

MIN_LIMIT = 1
MAX_LIMIT = 40  # EDGAR's count page cap
MIN_BODY_CHARS = 1
MAX_BODY_CHARS_CAP = 500_000  # sane safety cap per call
MAX_OFFSET_CHARS = 10_000_000

BODY_FETCH_TIMEOUT = 60.0  # EDGAR 10-K HTML can be 10+ MB; 10s default is too tight

_log = logging.getLogger("research_daemon.fetch_sec_filing")


def fetch_sec_filing(
    ticker: str,
    filing_type: str,
    limit: int = 3,
    *,
    include_body: bool = False,
    max_body_chars: int = 50_000,
    offset_chars: int = 0,
    config: Config | None = None,
    client: HttpClient | None = None,
) -> dict[str, Any]:
    """Return recent SEC filings of `filing_type` for `ticker`."""
    # Validation
    if not isinstance(ticker, str) or not ticker.strip():
        return build_error(status="error", source="edgar",
                           detail="ticker must be a non-empty string")
    if not isinstance(filing_type, str) or not filing_type.strip():
        return build_error(status="error", source="edgar",
                           detail="filing_type must be a non-empty string")
    if not isinstance(limit, int) or isinstance(limit, bool):
        return build_error(status="error", source="edgar",
                           detail="limit must be an integer")
    if limit < MIN_LIMIT or limit > MAX_LIMIT:
        return build_error(status="error", source="edgar",
                           detail=f"limit must be between {MIN_LIMIT} and {MAX_LIMIT}")
    if not isinstance(include_body, bool):
        return build_error(status="error", source="edgar",
                           detail="include_body must be a bool")
    if not isinstance(max_body_chars, int) or isinstance(max_body_chars, bool):
        return build_error(status="error", source="edgar",
                           detail="max_body_chars must be an integer")
    if max_body_chars < MIN_BODY_CHARS or max_body_chars > MAX_BODY_CHARS_CAP:
        return build_error(
            status="error", source="edgar",
            detail=f"max_body_chars must be between {MIN_BODY_CHARS} and {MAX_BODY_CHARS_CAP}",
        )
    if not isinstance(offset_chars, int) or isinstance(offset_chars, bool):
        return build_error(status="error", source="edgar",
                           detail="offset_chars must be an integer")
    if offset_chars < 0 or offset_chars > MAX_OFFSET_CHARS:
        return build_error(
            status="error", source="edgar",
            detail=f"offset_chars must be between 0 and {MAX_OFFSET_CHARS}",
        )

    symbol = ticker.strip().upper()
    normalised_form = filing_type.strip().upper()
    cfg = config or Config.from_env()
    http = client or HttpClient(user_agent=cfg.edgar_user_agent)

    # Primary: browse-edgar atom feed
    try:
        xml_text = http.get_text(
            EDGAR_BROWSE_URL,
            params={
                "action": "getcompany",
                "CIK": symbol,
                "type": normalised_form,
                "dateb": "",
                "owner": "include",
                "count": limit,
                "output": "atom",
            },
        )
    except NotFound:
        return build_error(status="not_found", source="edgar",
                           detail=f"ticker {symbol} not found")
    except RateLimited as exc:
        return build_error(status="rate_limited", source="edgar", detail=str(exc))
    except TransportError as exc:
        return build_error(status="error", source="edgar", detail=str(exc))

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        return build_error(status="error", source="edgar",
                           detail=f"failed to parse EDGAR atom feed: {exc}")

    cik = _extract_cik(root)
    entries = root.findall("atom:entry", ATOM_NS)

    items: list[dict[str, Any]] = []
    dropped = 0
    body_failures = 0
    parse_drop_warning_emitted = False

    for entry in entries:
        meta = _parse_entry(entry)
        if meta is None:
            dropped += 1
            continue

        item = _build_item(meta, cik=cik, offset_chars=offset_chars,
                           max_body_chars=max_body_chars)

        if include_body:
            _populate_body(item, meta, http, offset_chars, max_body_chars)
            if item.get("body_error") is not None:
                body_failures += 1

        items.append(item)

    data: dict[str, Any] = {
        "ticker": symbol,
        "filing_type": normalised_form,
        "limit": limit,
        "cik": cik,
        "include_body": include_body,
        "max_body_chars": max_body_chars,
        "offset_chars": offset_chars,
        "item_count": len(items),
        "dropped_count": dropped,
        "body_failure_count": body_failures,
        "items": items,
    }

    warnings: list[dict[str, Any]] = []
    completeness = _compute_completeness(
        include_body=include_body, dropped=dropped, body_failures=body_failures
    )

    if dropped > 0:
        _log.warning(
            "dropped %d malformed atom entr(ies) for %s %s",
            dropped, symbol, normalised_form,
        )
        warnings.append(make_warning(
            field="items",
            reason="parse_error",
            source="edgar",
            suggestion=f"{dropped} atom entr(ies) dropped; see data.dropped_count",
        ))
        parse_drop_warning_emitted = True

    if body_failures > 0:
        _log.warning(
            "body fetch failed for %d of %d filing(s) for %s",
            body_failures, len(items), symbol,
        )
        warnings.append(make_warning(
            field="items.body",
            reason="upstream_error",
            source="edgar",
            suggestion=(
                f"{body_failures} of {len(items)} body fetches failed; "
                "see each item.body_error"
            ),
        ))

    # Silence unused-variable lint for parse_drop_warning_emitted — it's a
    # logical marker that could be extended later; harmless to keep.
    _ = parse_drop_warning_emitted

    return build_ok(data, source="edgar", data_completeness=completeness,
                    warnings=warnings)


# ---------------------------------------------------------------------------
# Atom parsing
# ---------------------------------------------------------------------------

def _extract_cik(root: ET.Element) -> str | None:
    """Pull the company CIK from the <company-info> block (best-effort)."""
    # company-info and its children inherit the atom namespace.
    ci = root.find("atom:company-info", ATOM_NS)
    if ci is None:
        return None
    cik_el = ci.find("atom:cik", ATOM_NS)
    if cik_el is None or cik_el.text is None:
        return None
    return cik_el.text.strip() or None


def _parse_entry(entry: ET.Element) -> dict[str, Any] | None:
    """Extract the required fields from a <entry>. Returns None if unusable."""
    content = entry.find("atom:content", ATOM_NS)
    if content is None:
        return None

    accession = _child_text(content, "accession-number")
    filing_date_raw = _child_text(content, "filing-date")
    filing_type = _child_text(content, "filing-type")
    filing_href = _child_text(content, "filing-href")
    form_name = _child_text(content, "form-name")

    if not accession or not filing_type or not filing_href:
        return None

    parsed_date = _parse_edgar_date(filing_date_raw)
    if parsed_date is None:
        return None

    return {
        "accession_number": accession.strip(),
        "filing_type": filing_type.strip(),
        "filing_href": filing_href.strip(),
        "form_name": form_name.strip() if form_name else None,
        "filed_at_unix": parsed_date[0],
        "filed_at": parsed_date[1],
    }


def _child_text(parent: ET.Element, local_name: str) -> str | None:
    """Find a child element by local name (ignoring namespace)."""
    # Inside <content>, children inherit the atom namespace per XML inheritance.
    el = parent.find(f"atom:{local_name}", ATOM_NS)
    if el is None:
        return None
    return el.text


def _parse_edgar_date(s: Any) -> tuple[int, str] | None:
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        d = date.fromisoformat(s.strip()[:10])
    except ValueError:
        return None
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return int(dt.timestamp()), f"{d.isoformat()}T00:00:00Z"


# ---------------------------------------------------------------------------
# Per-item assembly
# ---------------------------------------------------------------------------

def _build_item(
    meta: dict[str, Any],
    *,
    cik: str | None,
    offset_chars: int,
    max_body_chars: int,
) -> dict[str, Any]:
    return {
        "accession_number": meta["accession_number"],
        "cik": cik,
        "filing_type": meta["filing_type"],
        "form_name": meta["form_name"],
        "filed_at_unix": meta["filed_at_unix"],
        "filed_at": meta["filed_at"],
        "index_url": meta["filing_href"],
        "primary_document_url": None,
        "body": None,
        "body_offset_chars": offset_chars,
        "body_returned_chars": 0,
        "body_total_chars": None,
        "body_truncated": False,
        "body_error": None,
    }


def _populate_body(
    item: dict[str, Any],
    meta: dict[str, Any],
    http: HttpClient,
    offset_chars: int,
    max_body_chars: int,
) -> None:
    """Mutate `item` to include body + related fields, or `body_error`."""
    folder_url = _folder_from_filing_href(meta["filing_href"])
    if folder_url is None:
        item["body_error"] = {
            "reason": "parse_error",
            "detail": f"could not derive folder URL from {meta['filing_href']}",
        }
        return

    try:
        index_payload = http.get_json(
            f"{folder_url}/index.json",
            timeout=BODY_FETCH_TIMEOUT,
        )
    except NotFound:
        item["body_error"] = {"reason": "not_found", "detail": f"{folder_url}/index.json 404"}
        return
    except RateLimited as exc:
        item["body_error"] = {"reason": "rate_limited", "detail": str(exc)}
        return
    except TransportError as exc:
        item["body_error"] = {"reason": "upstream_error", "detail": str(exc)}
        return

    primary_name = _find_primary_document(index_payload, meta["filing_type"])
    if primary_name is None:
        item["body_error"] = {
            "reason": "parse_error",
            "detail": f"no primary document of type {meta['filing_type']} in index.json",
        }
        return

    primary_url = f"{folder_url}/{primary_name}"
    item["primary_document_url"] = primary_url

    try:
        html = http.get_text(primary_url, timeout=BODY_FETCH_TIMEOUT)
    except NotFound:
        item["body_error"] = {"reason": "not_found", "detail": f"{primary_url} 404"}
        return
    except RateLimited as exc:
        item["body_error"] = {"reason": "rate_limited", "detail": str(exc)}
        return
    except TransportError as exc:
        item["body_error"] = {"reason": "upstream_error", "detail": str(exc)}
        return

    try:
        full_text = _extract_text(html)
    except Exception as exc:  # noqa: BLE001 — any bs4 failure is a parse error
        item["body_error"] = {"reason": "parse_error", "detail": f"text extraction failed: {exc}"}
        return

    total = len(full_text)
    item["body_total_chars"] = total

    start = min(offset_chars, total)
    end = min(start + max_body_chars, total)
    slice_ = full_text[start:end]
    item["body"] = slice_
    item["body_returned_chars"] = len(slice_)
    item["body_truncated"] = end < total


def _folder_from_filing_href(filing_href: str) -> str | None:
    """
    filing_href looks like:
      https://www.sec.gov/Archives/edgar/data/320193/000032019325000079/0000320193-25-000079-index.htm
    Folder URL:
      https://www.sec.gov/Archives/edgar/data/320193/000032019325000079
    """
    if "/" not in filing_href:
        return None
    folder = filing_href.rsplit("/", 1)[0]
    if not folder.startswith("http"):
        return None
    return folder


def _find_primary_document(index_payload: Any, filing_type: str) -> str | None:
    """Locate the primary document filename in EDGAR's index.json."""
    if not isinstance(index_payload, dict):
        return None
    directory = index_payload.get("directory")
    if not isinstance(directory, dict):
        return None
    items = directory.get("item")
    if not isinstance(items, list):
        return None

    # First pass: exact type match.
    target = filing_type.strip().upper()
    for it in items:
        if not isinstance(it, dict):
            continue
        item_type = it.get("type")
        item_name = it.get("name")
        if isinstance(item_type, str) and item_type.strip().upper() == target and \
                isinstance(item_name, str) and item_name.endswith((".htm", ".html")):
            return item_name

    # Fallback: first .htm that doesn't look like an SEC index page.
    for it in items:
        if not isinstance(it, dict):
            continue
        item_name = it.get("name")
        if not isinstance(item_name, str):
            continue
        low = item_name.lower()
        if low.endswith((".htm", ".html")) and "index" not in low and "header" not in low:
            return item_name

    return None


def _extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    lines = (line.strip() for line in text.splitlines())
    non_empty = (line for line in lines if line)
    return "\n".join(non_empty)


# ---------------------------------------------------------------------------
# Completeness
# ---------------------------------------------------------------------------

def _compute_completeness(
    *, include_body: bool, dropped: int, body_failures: int
) -> Completeness:
    if dropped > 0 or body_failures > 0:
        return "partial"
    if not include_body:
        return "metadata_only"
    return "complete"
