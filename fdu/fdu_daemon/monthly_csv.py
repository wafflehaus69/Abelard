"""The SEC monthly bulk CSV: a second, richer view of Form ADV Part 1A.

Two SEC products carry adviser data and they are not the same thing:

  ``reports.adviserinfo.sec.gov``  DAILY XML, ~20 usable fields, no Item 4,
                                   no Schedule A. This is the change detector.
  ``www.sec.gov`` (this module)    MONTHLY CSV, 448 columns including Item 4
                                   successions and 48 columns of Item 11
                                   disciplinary detail. Still no Schedule A.

The monthly product is strictly additive. It cannot drive change detection --
a month is far too coarse, and the daily feed already does that job -- but it
carries fields the daily feed simply does not have, at zero marginal fetch cost
beyond one 5 MB download a month.

**Access requires a declared contact address.** ``www.sec.gov`` returns 403 to a
User-Agent without one, from any network. That was established the hard way:
three UAs across two networks all failed, which read as evidence of a non-UA
cause, and was in fact three instances of the same omission. Set ``FDU_CONTACT``
and ``config.USER_AGENT`` carries it.

What the CSV does NOT solve: Schedule A ownership is absent here as well, so the
per-firm document leg survives for ownership structure.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from dataclasses import dataclass

from .errors import FeedParseError
from .fetch import Fetcher

MONTHLY_INDEX_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/"
    "information-about-registered-investment-advisers-exempt-reporting-advisers"
)

#: The current monthly product lives under ``investment/data/other/``. A LOOSER
#: pattern also matched ``frequently-requested-foia-document-information-about-
#: registered-investment-advisers-and-exempt/``, which is a HISTORICAL archive
#: going back to 2006 in a different format (pipe-delimited .txt, different
#: column names). Trusting the page's link order then selected a 2006 file as
#: "latest". Both mistakes are fixed here: constrain the family, and pick by a
#: date parsed from the filename rather than by position on the page.
_HREF_RE = re.compile(
    r'href="(/files/investment/data/other/'
    r'information-about-registered-investment-advisers-exempt-reporting-advisers/'
    r'ia[^"]*\.zip)"',
    re.I,
)

#: Filenames carry a date with NO separator and in two observed widths:
#: ``ia08032026_0.zip`` (MMDDYYYY) and ``ia030226.zip`` (MMDDYY). A trailing
#: ``_0`` marks a re-upload. Parsed to a sortable tuple; anything unparseable
#: sorts last rather than being silently treated as recent.
_DATE_RE = re.compile(r"/ia(\d{6}|\d{8})(?:-exempt)?(?:_\d+)?\.zip$", re.I)


def _file_date(path: str) -> tuple[int, int, int]:
    m = _DATE_RE.search(path)
    if not m:
        return (0, 0, 0)
    digits = m.group(1)
    if len(digits) == 8:
        mm, dd, yyyy = int(digits[:2]), int(digits[2:4]), int(digits[4:])
    else:
        mm, dd, yy = int(digits[:2]), int(digits[2:4]), int(digits[4:])
        yyyy = 2000 + yy
    return (yyyy, mm, dd)

#: Column names we lift. The CSV has 448; carrying all of them would make the
#: ledger a copy of the file rather than a reading of it. These are the ones the
#: daily feed cannot supply.
COLUMNS = {
    "crd": "Organization CRD#",
    "acquired_name": "Acquired Firm",
    "acquired_sec_no": "Acquired Firm SEC#",
    "acquired_crd": "Acquired Firm CRD#",
    "acquired_count": "Total Number of Acquired Firms",
    "latest_filing": "Latest ADV Filing Date",
    "sec_status": "SEC Current Status",
    "sec_status_date": "SEC Status Effective Date",
    "relying_advisers": "Total number of relying advisers",
    "control_related": "Control/Controlled by Related Person",
    "common_control": "Under Common Control",
}


@dataclass
class MonthlyRow:
    crd: str
    acquired_name: str | None = None
    acquired_sec_no: str | None = None
    acquired_crd: str | None = None
    acquired_count: str | None = None
    latest_filing: str | None = None
    sec_status: str | None = None
    sec_status_date: str | None = None
    relying_advisers: str | None = None
    control_related: str | None = None
    common_control: str | None = None

    @property
    def has_succession(self) -> bool:
        return bool((self.acquired_name or "").strip())

    @property
    def is_self_succession(self) -> bool | None:
        """Acquired CRD equal to the filer's own is a reorganisation, not a sale.

        Measured across the whole corpus via the per-firm documents: 14 of 15
        filed successions were self-successions. Without this discriminator the
        succession signal is ~94% re-incorporations.
        """
        if not self.has_succession:
            return None
        acq = (self.acquired_crd or "").strip()
        if not acq:
            return None
        return acq == self.crd.strip()


def latest_monthly_urls(fetcher: Fetcher) -> list[str]:
    """Return absolute URLs for the most recent registered + exempt ZIPs."""
    html = fetcher.get_text(MONTHLY_INDEX_URL, surface="sec_monthly_index")
    paths = _HREF_RE.findall(html)
    if not paths:
        raise FeedParseError(
            f"no monthly IA data links found at {MONTHLY_INDEX_URL} "
            f"({len(html)} bytes fetched) -- page layout may have changed"
        )
    registered = sorted((p for p in paths if "exempt" not in p.lower()), key=_file_date)
    exempt = sorted((p for p in paths if "exempt" in p.lower()), key=_file_date)
    picked = [x for x in (registered[-1:] or [None]) + (exempt[-1:] or [None]) if x]
    unparsed = [p for p in picked if _file_date(p) == (0, 0, 0)]
    if unparsed:
        raise FeedParseError(
            f"cannot date-parse monthly filename(s) {unparsed}; refusing to guess which is latest"
        )
    return [f"https://www.sec.gov{p}" for p in picked]


def parse_zip(payload: bytes) -> list[MonthlyRow]:
    """Parse one monthly ZIP into rows. Raises rather than half-parsing."""
    try:
        z = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile as exc:
        raise FeedParseError(f"monthly bulk payload is not a zip ({len(payload)} bytes)") from exc
    names = [n for n in z.namelist() if n.lower().endswith(".csv")]
    if not names:
        raise FeedParseError(f"monthly zip carries no CSV; contents: {z.namelist()[:5]}")

    rows: list[MonthlyRow] = []
    with z.open(names[0]) as fh:
        reader = csv.reader(io.TextIOWrapper(fh, encoding="latin-1"))
        header = next(reader, None)
        if not header:
            raise FeedParseError(f"{names[0]} has no header row")
        idx = {}
        for key, col in COLUMNS.items():
            if col in header:
                idx[key] = header.index(col)
        if "crd" not in idx:
            raise FeedParseError(
                f"{names[0]} has no '{COLUMNS['crd']}' column; got {len(header)} columns"
            )
        for raw in reader:
            if len(raw) <= idx["crd"]:
                continue
            crd = raw[idx["crd"]].strip()
            if not crd:
                continue
            kw = {}
            for key, i in idx.items():
                if key == "crd":
                    continue
                kw[key] = raw[i].strip() if i < len(raw) and raw[i].strip() else None
            rows.append(MonthlyRow(crd=crd, **kw))
    if not rows:
        raise FeedParseError(f"{names[0]} parsed to zero rows -- refusing to treat as empty-ok")
    return rows


def fetch_monthly(fetcher: Fetcher) -> list[MonthlyRow]:
    """Fetch and parse the latest monthly bulk files (registered + exempt)."""
    rows: list[MonthlyRow] = []
    for url in latest_monthly_urls(fetcher):
        payload = fetcher.get_bytes(url, surface="sec_monthly_zip")
        rows.extend(parse_zip(payload))
    return rows
