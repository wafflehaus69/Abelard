"""Monthly bulk CSV tests, built on the shape the file actually has."""

from __future__ import annotations

import csv
import io
import zipfile

import pytest

from fdu_daemon.errors import FeedParseError
from fdu_daemon.monthly_csv import MonthlyRow, parse_zip

HEADER = ["SEC Region", "Organization CRD#", "Legal Name", "Latest ADV Filing Date",
          "SEC Current Status", "SEC Status Effective Date",
          "Total number of relying advisers", "Control/Controlled by Related Person",
          "Under Common Control", "Acquired Firm", "Acquired Firm SEC#",
          "Acquired Firm CRD#", "Total Number of Acquired Firms"]


def _zip(rows, name="IA_SEC_FIRM_ROSTER.CSV"):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(HEADER)
    for r in rows:
        w.writerow(r)
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr(name, buf.getvalue())
    return out.getvalue()


def _row(crd, acq_name="", acq_crd=""):
    return ["NYRO", crd, "EXAMPLE ADVISORS LLC", "2026-08-01", "APPROVED",
            "2020-01-01", "0", "N", "N", acq_name, "", acq_crd, "1" if acq_name else ""]


def test_parses_rows_and_lifts_named_columns():
    rows = parse_zip(_zip([_row("123456"), _row("654321")]))
    assert [r.crd for r in rows] == ["123456", "654321"]
    assert rows[0].sec_status == "APPROVED"
    assert rows[0].latest_filing == "2026-08-01"


def test_third_party_succession_detected():
    rows = parse_zip(_zip([_row("157514", "MARSTONE LLC", "164810")]))
    r = rows[0]
    assert r.has_succession
    assert r.is_self_succession is False, "different CRD means a real change of hands"


def test_self_succession_detected():
    """14 of 15 filed successions in the corpus were the filer's own CRD."""
    rows = parse_zip(_zip([_row("111936", "GREYCOURT & CO., INC.", "111936")]))
    r = rows[0]
    assert r.has_succession
    assert r.is_self_succession is True, "same CRD is a reorganisation, not a sale"


def test_no_succession_returns_none_not_false():
    """Absence must not read as 'not a self-succession'."""
    r = parse_zip(_zip([_row("123456")]))[0]
    assert r.has_succession is False
    assert r.is_self_succession is None


def test_succession_without_crd_is_undetermined():
    """A named firm with no CRD cannot be classified; refuse rather than guess."""
    r = parse_zip(_zip([_row("123456", "SOME FIRM LLC", "")]))[0]
    assert r.has_succession is True
    assert r.is_self_succession is None


def test_blank_crd_rows_skipped():
    assert len(parse_zip(_zip([_row("123456"), _row("")]))) == 1


def test_empty_csv_raises_rather_than_returning_empty():
    with pytest.raises(FeedParseError):
        parse_zip(_zip([]))


def test_non_zip_payload_raises():
    with pytest.raises(FeedParseError):
        parse_zip(b"not a zip at all")


def test_zip_without_csv_raises():
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr("readme.txt", "nothing here")
    with pytest.raises(FeedParseError):
        parse_zip(out.getvalue())


def test_missing_crd_column_raises():
    buf = io.StringIO()
    csv.writer(buf).writerow(["Some Other Column"])
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr("x.CSV", buf.getvalue())
    with pytest.raises(FeedParseError):
        parse_zip(out.getvalue())


def test_monthly_row_is_not_person_shaped():
    import re

    fields = set(MonthlyRow(crd="1").__dict__)
    person = re.compile(r"(first|last|middle)_?name|email|phone|dob|ssn|individual", re.I)
    assert not [f for f in fields if person.search(f)]
