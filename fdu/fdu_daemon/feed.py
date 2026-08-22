"""Parse the IAPD bulk compilation feeds into firm records.

The feeds are a daily SNAPSHOT, not an event stream: each firm carries only its
most recent filing date. Every longitudinal signal therefore comes from diffing
snapshots we archived ourselves, and the publisher keeps only ~8 days, so a gap
in our archive is permanent.

Two feeds share one shape:
  IA_FIRM_SEC_Feed   -> <IAPDFirmSECReport>   SEC-registered advisers and ERAs
  IA_FIRM_STATE_Feed -> <IAPDFirmStateReport> state-registered advisers

Neither carries Item 4 (Successions), Schedule A/B (ownership), or the DRP
detail pages. Measured 2026-08-21: zero occurrences across all 23,794 SEC
records. Those live only in the per-firm document -- see ``adv_pdf``.
"""

from __future__ import annotations

import gzip
import re
from dataclasses import dataclass, field
from pathlib import Path
from xml.etree import ElementTree as ET

from .errors import FeedParseError

_MANIFEST_NAME_RE = re.compile(r"^IA_(FIRM_SEC|FIRM_STATE|INDVL)_Feed_(\d{2})_(\d{2})_(\d{4})\.xml\.(gz|zip)$")


@dataclass
class FirmRecord:
    """One adviser firm as the bulk feed presents it on one day.

    Every field is nullable because the feed is genuinely heterogeneous: Exempt
    Reporting Advisers complete only part of the form. A missing value here
    means NOT APPLICABLE, not zero -- see ``absence_reason``.
    """

    crd: str
    source_feed: str
    legal_name: str | None = None
    business_name: str | None = None
    sec_number: str | None = None
    umbrella: str | None = None

    rgstn_type: str | None = None
    rgstn_status: str | None = None
    rgstn_date: str | None = None

    filing_date: str | None = None
    form_version: str | None = None

    city: str | None = None
    state: str | None = None
    country: str | None = None
    postal_code: str | None = None

    total_employees: int | None = None
    advisory_employees: int | None = None
    aum_total: int | None = None
    aum_discretionary: int | None = None
    aum_non_discretionary: int | None = None
    accounts_total: int | None = None
    clients_hnw: int | None = None
    disciplinary_flag: str | None = None

    #: Notice-filed state regulator codes. Stored SORTED -- the publisher emits
    #: these children in unstable order and 92.5% of raw-byte diff hits are
    #: that artifact alone. See ``normalize``.
    notice_states: tuple[str, ...] = ()

    @property
    def is_era(self) -> bool:
        return (self.rgstn_type or "").upper() == "ERA"

    @property
    def absence_reason(self) -> str | None:
        """Why a numeric field is missing, when it is.

        Distinguishing "not applicable" from "zero" is load-bearing: a score
        computed over fields would otherwise read an ERA's missing headcount as
        a calm small firm.
        """
        if self.total_employees is None and self.is_era:
            return "era_partial_form"
        if self.total_employees is None:
            return "not_reported"
        return None


def _i(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_manifest(payload: dict) -> dict[str, dict]:
    """Index the compilation manifest by feed kind.

    The advertised ``size`` is the UNCOMPRESSED size and is roughly 10.7x the
    download. It is carried through verbatim and labelled, never used as a
    transfer estimate.
    """
    files = payload.get("files")
    if not isinstance(files, list) or not files:
        raise FeedParseError(f"manifest carried no file list: {payload!r}")
    out: dict[str, dict] = {}
    for entry in files:
        name = entry.get("name", "")
        m = _MANIFEST_NAME_RE.match(name)
        if not m:
            continue
        kind, mm, dd, yyyy, _ext = m.groups()
        out[kind] = {
            "name": name,
            "kind": kind,
            "date": f"{yyyy}-{mm}-{dd}",
            "uncompressed_size_label": entry.get("size"),
        }
    if not out:
        raise FeedParseError(f"manifest had {len(files)} entries, none matching the known feed pattern")
    return out


def _firm_from_element(el: ET.Element, source_feed: str) -> FirmRecord | None:
    info = el.find("Info")
    if info is None:
        return None
    crd = info.get("FirmCrdNb")
    if not crd:
        return None

    rec = FirmRecord(
        crd=crd,
        source_feed=source_feed,
        legal_name=info.get("LegalNm"),
        business_name=info.get("BusNm"),
        sec_number=info.get("SECNb") or info.get("FirmIaFullSecNb"),
        umbrella=info.get("UmbrRgstn"),
    )

    rgstn = el.find("Rgstn")
    if rgstn is not None:
        rec.rgstn_type = rgstn.get("FirmType")
        rec.rgstn_status = rgstn.get("St")
        rec.rgstn_date = rgstn.get("Dt")
    else:
        # State feed nests registration under StateRgstn/Rgltrs/Rgltr.
        rgltr = el.find("./StateRgstn/Rgltrs/Rgltr")
        if rgltr is not None:
            rec.rgstn_type = "STATE"
            rec.rgstn_status = rgltr.get("St")
            rec.rgstn_date = rgltr.get("Dt")

    filing = el.find("Filing")
    if filing is not None:
        rec.filing_date = filing.get("Dt")
        rec.form_version = filing.get("FormVrsn")

    addr = el.find("MainAddr")
    if addr is not None:
        rec.city = addr.get("City")
        rec.state = addr.get("State")
        rec.country = addr.get("Cntry")
        rec.postal_code = addr.get("PostlCd")

    part = el.find("./FormInfo/Part1A")
    if part is None:
        part = el.find("FormInfo")
    if part is not None:
        i5a = part.find("Item5A")
        if i5a is not None:
            rec.total_employees = _i(i5a.get("TtlEmp"))
        i5b = part.find("Item5B")
        if i5b is not None:
            rec.advisory_employees = _i(i5b.get("Q5B1"))
        i5d = part.find("Item5D")
        if i5d is not None:
            rec.clients_hnw = _i(i5d.get("Q5DB1"))
        i5f = part.find("Item5F")
        if i5f is not None:
            rec.aum_discretionary = _i(i5f.get("Q5F2A"))
            rec.aum_non_discretionary = _i(i5f.get("Q5F2B"))
            rec.aum_total = _i(i5f.get("Q5F2C"))
            rec.accounts_total = _i(i5f.get("Q5F2F"))
        i11 = part.find("Item11")
        if i11 is not None:
            rec.disciplinary_flag = i11.get("Q11")

    states = [s.get("RgltrCd") for s in el.findall("./NoticeFiled/States")]
    states += [s.get("Cd") for s in el.findall("./StateRgstn/Rgltrs/Rgltr")]
    rec.notice_states = tuple(sorted({s for s in states if s}))

    return rec


def parse_feed(path: Path) -> list[FirmRecord]:
    """Parse one gzipped bulk feed into firm records.

    Raises rather than returning a partial parse: a half-read corpus that looks
    complete would silently under-report every delta computed against it.
    """
    try:
        with gzip.open(path, "rb") as fh:
            raw = fh.read()
    except OSError as exc:
        raise FeedParseError(f"cannot read feed {path}: {exc}") from exc

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise FeedParseError(f"feed {path.name} is not well-formed XML: {exc}") from exc

    tag = root.tag
    if tag not in ("IAPDFirmSECReport", "IAPDFirmStateReport"):
        raise FeedParseError(f"unexpected feed root <{tag}> in {path.name}")
    source_feed = "sec" if tag == "IAPDFirmSECReport" else "state"

    records: list[FirmRecord] = []
    for el in root.iter("Firm"):
        rec = _firm_from_element(el, source_feed)
        if rec is not None:
            records.append(rec)

    if not records:
        raise FeedParseError(f"feed {path.name} parsed to zero firms -- refusing to treat as empty-ok")

    gen_on = root.get("GenOn")
    for rec in records:
        rec.__dict__.setdefault("_gen_on", gen_on)
    return records


def feed_generated_on(path: Path) -> str | None:
    """Read the feed's own GenOn stamp without parsing the whole corpus."""
    with gzip.open(path, "rb") as fh:
        head = fh.read(400).decode("ISO-8859-1", errors="replace")
    m = re.search(r'GenOn="([^"]+)"', head)
    return m.group(1) if m else None
