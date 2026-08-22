"""Extract firm-level facts from a per-firm Form ADV document.

Why this module exists: Item 4 (Successions), Schedule A/B (ownership) and the
DRP pages are absent from the bulk feed -- measured at zero occurrences across
all 23,794 SEC records -- and they carry the succession signal. They exist only
here.

**Nothing is retained.** The document is fetched into memory, reduced to the
structural facts below, and dropped. Documents average 1.98 MB and the corpus
is ~49 GB; the extracted rows are tens of MB for the whole population.

**I-3 boundary, enforced in this module.** Schedule A lists named individuals
with titles, CRD numbers and dates of birth. Those values are read in order to
be counted and are never returned. What leaves this module is: how many owners,
what ownership-percentage codes, how many control persons, which acquisition
dates. That is enough to detect an ownership CHANGE -- which is the signal --
without building the per-person dossier the invariant forbids.

Two document quirks cost real time to find and both are load-bearing here.

1. **Page text is cumulative.** Page N's extracted text contains pages 0..N, so
   the tail page of a run carries that whole run exactly once. Extracting every
   page costs O(n^2) and yields each section n times. Measured: tail-only is
   ~0.3s against 4-10s for a full walk.

2. **A large filing contains several runs concatenated** -- a mega-adviser with
   relying advisers. Run starts are detectable for free from raw content-stream
   length (they spike well above the ~212-byte median), verified on documents
   from 24 to 1,033 pages with the first boundary at page 24 every time. The
   base Part 1A is always run 0, which is where Item 4 and Schedule A live.

3. **Item 4's checkbox is not recoverable.** These PDFs carry no AcroForm fields
   and the tick is drawn, not written, so ``extract_text`` yields the blank
   question for answered and unanswered filings alike. The recoverable signal is
   whether Schedule D **Section 4 Successions** carries content or reads
   ``No Information Filed``.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field

import pypdf

from . import config
from .errors import ExtractError
from .fetch import Fetcher

#: A page whose raw content stream exceeds this multiple of the document median
#: is treated as the start of a new cumulative run.
RUN_START_RATIO = 1.4

#: Cap on how many run tails we will lay out as text. The base run and the final
#: run are always included; the rest are read until the sections we need appear.
MAX_RUN_TAILS = 8

#: Form ADV Schedule A ownership codes, least to most.
OWNERSHIP_CODE_ORDER = ("NA", "A", "B", "C", "D", "E", "F")

#: Tail of an owner row:  MM/YYYY  <ownership code>  <control Y/N>  <PR Y/N>
_OWNER_ROW_RE = re.compile(r"(\d{2}/\d{4})\s+(NA|[A-F])\s+([YN])\s+([YN])")

_TABLE_HEADER = "FULL LEGAL NAME"
_SCHED_A_HEAD = "Direct Owners and Executive Officers"
_SCHED_B_HEAD = "Indirect Owners"
_NO_INFO = "No Information Filed"

_REQUIRED_MARKERS = ("SECTION 4 Successions", _SCHED_A_HEAD)


@dataclass
class AdvFacts:
    """Structural facts only. No names, no addresses, no per-person rows."""

    crd: str
    doc_bytes: int = 0
    doc_pages: int = 0
    pages_read: int = 0
    truncated: bool = False
    form_filed_at: str | None = None
    amendment_type: str | None = None

    section4_filed: bool | None = None
    succession_detail: str | None = None

    direct_owner_count: int | None = None
    indirect_owner_count: int | None = None
    ownership_codes: list[str] = field(default_factory=list)
    max_ownership_code: str | None = None
    control_person_count: int | None = None
    ownership_acquired: list[str] = field(default_factory=list)

    extract_status: str = "ok"
    extract_note: str | None = None

    def as_row(self, fetched_unix: int) -> dict:
        return {
            "crd": self.crd,
            "fetched_unix": fetched_unix,
            "doc_bytes": self.doc_bytes,
            "doc_pages": self.doc_pages,
            "form_filed_at": self.form_filed_at,
            "amendment_type": self.amendment_type,
            "section4_filed": None if self.section4_filed is None else int(self.section4_filed),
            "succession_detail": self.succession_detail,
            "direct_owner_count": self.direct_owner_count,
            "indirect_owner_count": self.indirect_owner_count,
            "ownership_codes": ",".join(self.ownership_codes) or None,
            "max_ownership_code": self.max_ownership_code,
            "control_person_count": self.control_person_count,
            "extract_status": self.extract_status,
            "extract_note": self.extract_note,
        }


# --------------------------------------------------------------------------
# Document text
# --------------------------------------------------------------------------


def _content_sizes(reader: pypdf.PdfReader) -> list[int]:
    sizes = []
    for page in reader.pages:
        try:
            sizes.append(len(page.get_contents().get_data()))
        except Exception:
            sizes.append(-1)
    return sizes


def run_tail_pages(sizes: list[int]) -> list[int]:
    """Page indices that end a cumulative run, in priority order.

    Base run first (it carries Part 1A), then the final page, then the rest.
    """
    n = len(sizes)
    if n == 0:
        return []
    usable = sorted(s for s in sizes if s > 0)
    if not usable:
        return [n - 1]
    median = usable[len(usable) // 2]
    starts = [i for i, s in enumerate(sizes) if i > 0 and s > median * RUN_START_RATIO]
    tails = [s - 1 for s in starts if s - 1 >= 0]
    tails.append(n - 1)
    seen, ordered = set(), []
    for idx in tails:
        if idx not in seen:
            seen.add(idx)
            ordered.append(idx)
    ordered.sort()
    if len(ordered) <= 2:
        return ordered
    # base run, final run, then the middle ones
    return [ordered[0], ordered[-1]] + ordered[1:-1]


def _document_text(reader: pypdf.PdfReader) -> tuple[str, int, str | None]:
    """Return (text, pages_laid_out, note)."""
    n = len(reader.pages)
    if n == 0:
        return "", 0, "document has no pages"
    tails = run_tail_pages(_content_sizes(reader))
    chunks: list[str] = []
    read = 0
    for idx in tails[:MAX_RUN_TAILS]:
        chunks.append(reader.pages[idx].extract_text() or "")
        read += 1
        if all(any(m in c for c in chunks) for m in _REQUIRED_MARKERS):
            break
    text = "\n".join(chunks)
    missing = [m for m in _REQUIRED_MARKERS if m not in text]
    note = None
    if missing:
        note = f"read {read} of {len(tails)} run tails; sections not found: {', '.join(missing)}"
    return text, read, note


# --------------------------------------------------------------------------
# Ownership tables
# --------------------------------------------------------------------------


def _owner_tables(text: str) -> dict[str, list[str]]:
    """Locate owner tables and classify each as Schedule A or B.

    Anchored on the table header rather than on schedule headings, because
    "Schedule B" appears inside Schedule A's own instructional preamble -- a
    heading-to-heading slice ends before the data and silently reports zero
    owners for a firm that plainly has two. That was a real bug here.
    """
    out: dict[str, list[str]] = {"A": [], "B": []}
    positions = [m.start() for m in re.finditer(re.escape(_TABLE_HEADER), text)]
    for k, pos in enumerate(positions):
        before = text[:pos]
        ia = before.rfind(_SCHED_A_HEAD)
        ib = before.rfind(_SCHED_B_HEAD)
        kind = "B" if ib > ia else "A"
        end = positions[k + 1] if k + 1 < len(positions) else len(text)
        for marker in ("Schedule C", "Schedule D", "SECTION "):
            m = text.find(marker, pos)
            if 0 < m < end:
                end = m
        out[kind].append(text[pos:end])
    return out


def _parse_owner_block(blocks: list[str]) -> tuple[int, list[str], int, list[str]]:
    """Count owners and collect ownership codes. Names are read and dropped."""
    codes: list[str] = []
    controls = 0
    acquired: list[str] = []
    for block in blocks:
        for m in _OWNER_ROW_RE.finditer(block):
            date, code, control, _pr = m.groups()
            codes.append(code)
            acquired.append(date)
            if control == "Y":
                controls += 1
    return len(codes), codes, controls, acquired


def _section(text: str, start: str, *ends: str) -> str | None:
    i = text.find(start)
    if i < 0:
        return None
    j = len(text)
    for end in ends:
        k = text.find(end, i + len(start))
        if 0 < k < j:
            j = k
    return text[i:j]


# --------------------------------------------------------------------------
# Entry points
# --------------------------------------------------------------------------


def extract_facts(crd: str, payload: bytes) -> AdvFacts:
    """Reduce one ADV document to structural facts.

    Raises ``ExtractError`` if the document is unreadable. A readable document
    that simply has no Schedule A returns a count of 0 with a status saying so --
    that is data, not failure.
    """
    facts = AdvFacts(crd=crd, doc_bytes=len(payload))
    try:
        reader = pypdf.PdfReader(io.BytesIO(payload))
        facts.doc_pages = len(reader.pages)
        text, pages_read, read_note = _document_text(reader)
        facts.pages_read = pages_read
        facts.truncated = read_note is not None
    except ExtractError:
        raise
    except Exception as exc:  # pypdf raises a wide family
        raise ExtractError(f"CRD {crd}: cannot parse ADV document ({exc})") from exc

    if not text.strip():
        raise ExtractError(f"CRD {crd}: ADV document parsed to empty text ({facts.doc_pages} pages)")

    m = re.search(r"(Annual Amendment|Other-Than-Annual Amendment|Initial Application)", text)
    if m:
        facts.amendment_type = m.group(1)
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s*[AP]M)", text)
    if m:
        facts.form_filed_at = m.group(1)

    # -- Successions: Schedule D Section 4 --------------------------------
    sec4 = _section(text, "SECTION 4 Successions", "Item 5", "SECTION 5", "SECTION 6")
    if sec4 is not None:
        body = sec4[len("SECTION 4 Successions"):].strip()
        facts.section4_filed = _NO_INFO not in body[:80]
        if facts.section4_filed:
            # Firm-level only: a predecessor adviser is a FIRM and the date is
            # an event date. No individuals appear in this section.
            facts.succession_detail = re.sub(r"\s+", " ", body[:400]).strip() or None

    # -- Ownership structure ----------------------------------------------
    tables = _owner_tables(text)
    if _SCHED_A_HEAD in text:
        n, codes, controls, acquired = _parse_owner_block(tables["A"])
        facts.direct_owner_count = n
        facts.ownership_codes = sorted(codes)
        facts.control_person_count = controls
        facts.ownership_acquired = sorted(set(acquired))
        if codes:
            facts.max_ownership_code = max(
                codes,
                key=lambda c: OWNERSHIP_CODE_ORDER.index(c) if c in OWNERSHIP_CODE_ORDER else -1,
            )
    if _SCHED_B_HEAD in text:
        n, _c, _ct, _a = _parse_owner_block(tables["B"])
        facts.indirect_owner_count = n

    notes = []
    if read_note:
        notes.append(read_note)
    if facts.direct_owner_count is None:
        notes.append("Schedule A not located")
    if facts.section4_filed is None:
        notes.append("Schedule D Section 4 not located")
    if notes:
        facts.extract_status = "partial"
        facts.extract_note = "; ".join(notes)

    return facts


def fetch_and_extract(fetcher: Fetcher, crd: str) -> AdvFacts:
    """Fetch one ADV document and reduce it. The payload is never written out."""
    url = config.ADV_PDF_TEMPLATE.format(crd=crd)
    payload = fetcher.get_bytes(url, surface="adv_pdf")
    try:
        return extract_facts(crd, payload)
    finally:
        del payload
