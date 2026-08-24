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
   from 24 to 1,750 pages with the first boundary at page 24 every time.
   The base Part 1A is run 0, but the **ownership schedules are at the END**,
   after every Schedule R block: on a 1,750-page / 60-run filing Section 4 sits
   in run 1 and Schedule A in run **57**. Read order matters -- see
   ``run_tail_pages``.

2b. **A 200 can be an absence.** The publisher serves a valid one-page PDF
   reading "A PDF version of the Form ADV is not available for this firm" for
   some filers. It parses fine and yields nothing, which would otherwise be
   recorded as a firm with no owners and no successions. It is detected and
   recorded as ``unavailable`` [E1].

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
MAX_RUN_TAILS = 12

#: Form ADV Schedule A ownership codes, least to most.
OWNERSHIP_CODE_ORDER = ("NA", "A", "B", "C", "D", "E", "F")

#: Tail of an owner row:  MM/YYYY  <ownership code>  <control Y/N>  <PR Y/N>
_OWNER_ROW_RE = re.compile(r"(\d{2}/\d{4})\s+(NA|[A-F])\s+([YN])\s+([YN])")

_TABLE_HEADER = "FULL LEGAL NAME"
_SCHED_A_HEAD = "Direct Owners and Executive Officers"
_SCHED_B_HEAD = "Indirect Owners"
_NO_INFO = "No Information Filed"

_REQUIRED_MARKERS = ("SECTION 4 Successions", _SCHED_A_HEAD)

#: Presence of an Item 4 heading distinguishes "this form HAS a successions item
#: and we failed to find it" from "this form variant does not have one".
#: Exempt Reporting Advisers complete only a subset of Form ADV -- Items 1, 2, 3,
#: 5, 6, 7, 10, 11 and no Item 4 -- so a succession field is NOT APPLICABLE to
#: them, not missing. Measured 2026-08-23: 6,644 of 6,663 ERAs in the corpus.
#: Recording that as an extraction failure buried a structural fact under a 30%
#: error rate and made a working run look broken.
_ITEM4_HEADING_RE = re.compile(r"Item\s*4(?![0-9])")

#: Schedule D Section 4 renders each acquired firm as label-then-value on the
#: following line. A first cut sliced the section and stored its first 400
#: characters -- which is the INSTRUCTION preamble, so every lead read "Complete
#: the following information if you are succeeding to..." and carried no facts.
#: Anchor on the labels, not on the heading.
_ACQ_NAME_RE = re.compile(r"Name of Acquired Firm[ \t]*\n[ \t]*(\S.*)")
_ACQ_SEC_RE = re.compile(r"Acquired Firm's SEC File No\.[^\n]*\n[ \t]*(\d{3}[ \t]*-[ \t]*\d+)")
_ACQ_CRD_RE = re.compile(r"Acquired Firm's CRD Number[ \t]*\n[ \t]*(\d+)")

#: The publisher serves a valid 200 with a valid one-page PDF that says the
#: filing is not available. That is an absence wearing a success costume [E1],
#: and it must be recorded as unavailable rather than silently parsed as an
#: empty filing.
_STUB_MARKER = "A PDF version of the Form ADV is not available for this firm"


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
    succession_count: int | None = None
    succession_acquired_names: list[str] = field(default_factory=list)
    succession_acquired_crds: list[str] = field(default_factory=list)
    #: True when EVERY acquired firm shares this filer's own CRD. That is an
    #: entity reorganisation -- an LLC conversion, a re-domicile -- and not a
    #: purchase of somebody else's practice. Measured on the first two filed
    #: successions in the corpus: both self. Without this the lead list presents
    #: a re-incorporation as an acquisition target.
    succession_is_self: bool | None = None

    direct_owner_count: int | None = None
    indirect_owner_count: int | None = None
    ownership_codes: list[str] = field(default_factory=list)
    max_ownership_code: str | None = None
    control_person_count: int | None = None
    ownership_acquired: list[str] = field(default_factory=list)

    extract_status: str = "ok"
    extract_note: str | None = None
    #: Set when a section is absent because this form VARIANT lacks it, rather
    #: than because extraction failed. Absence is data; failure is not.
    not_applicable: str | None = None

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
            "succession_count": self.succession_count,
            "succession_acquired_names": "; ".join(self.succession_acquired_names) or None,
            "succession_acquired_crds": ",".join(self.succession_acquired_crds) or None,
            "succession_is_self": None if self.succession_is_self is None else int(self.succession_is_self),
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
    """Page indices that end a cumulative run, in READ-PRIORITY order.

    The order matters and was got wrong once. In an umbrella filing the layout
    is: base Part 1A, then a Schedule R block per relying adviser, and the
    ownership schedules come at the **end**. Measured on a 1,750-page / 60-run
    filing: Section 4 sits in run 1, and Schedule A in **run 57**. An order of
    [first, last, then ascending] read runs 0, 59, 1, 2 ... and never reached
    it, reporting "Schedule A not located" for the largest advisers in the
    corpus.

    So: the two base runs first (Items 1-11 and Section 4), then walk back from
    the END (the ownership schedules). On that filing this finds both inside
    five page extractions.
    """
    n = len(sizes)
    if n == 0:
        return []
    usable = sorted(s for s in sizes if s > 0)
    if not usable:
        return [n - 1]
    median = usable[len(usable) // 2]
    starts = [i for i, s in enumerate(sizes) if i > 0 and s > median * RUN_START_RATIO]
    tails = sorted({s - 1 for s in starts if s - 1 >= 0} | {n - 1})
    if len(tails) <= 2:
        return tails
    head = tails[:2]
    rest = [idx for idx in reversed(tails) if idx not in head]
    return head + rest


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

    if _STUB_MARKER in text:
        # A 200 carrying a notice that the filing is unavailable. Recorded as an
        # absence, never parsed as a firm with no owners and no successions.
        facts.extract_status = "unavailable"
        facts.extract_note = "publisher served a stub: PDF version not available for this firm"
        return facts

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
            # Firm-level only: an acquired adviser is a FIRM. No individuals
            # appear in this section, so nothing here engages I-3.
            names = [n.strip() for n in _ACQ_NAME_RE.findall(sec4) if n.strip()]
            crds = [c.strip() for c in _ACQ_CRD_RE.findall(sec4)]
            secs = [re.sub(r"[ \t]", "", s) for s in _ACQ_SEC_RE.findall(sec4)]
            facts.succession_acquired_names = names
            facts.succession_acquired_crds = crds
            facts.succession_count = len(names) or None
            if crds:
                # Every acquired CRD equal to our own means this filing reports a
                # reorganisation of the same business -- an LLC conversion or a
                # re-domicile -- not the purchase of somebody else's practice.
                facts.succession_is_self = all(c == crd for c in crds)
            parts = []
            for i, nm in enumerate(names):
                bits = [nm]
                if i < len(crds):
                    bits.append(f"CRD {crds[i]}")
                if i < len(secs):
                    bits.append(secs[i])
                parts.append(" ".join(bits))
            facts.succession_detail = "; ".join(parts) or None

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

    # Separate "the form does not have this" from "we did not find it".
    notes: list[str] = []
    degraded = False

    if facts.direct_owner_count is None:
        notes.append("Schedule A not located")
        degraded = True

    if facts.section4_filed is None:
        if _ITEM4_HEADING_RE.search(text) is None:
            facts.not_applicable = "form variant omits Item 4 (ERA or subset filing)"
            notes.append(facts.not_applicable)
        else:
            notes.append("Schedule D Section 4 not located")
            degraded = True

    if read_note and degraded:
        notes.insert(0, read_note)

    if degraded:
        facts.extract_status = "partial"
    elif facts.not_applicable:
        facts.extract_status = "not_applicable"
    facts.extract_note = "; ".join(notes) or None

    return facts


def fetch_and_extract(fetcher: Fetcher, crd: str) -> AdvFacts:
    """Fetch one ADV document and reduce it. The payload is never written out."""
    url = config.ADV_PDF_TEMPLATE.format(crd=crd)
    payload = fetcher.get_bytes(url, surface="adv_pdf")
    try:
        return extract_facts(crd, payload)
    finally:
        del payload
