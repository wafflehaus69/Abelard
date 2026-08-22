"""Shared fixtures.

The Schedule A fixture is transcribed from a real Form ADV rendering so the
parser is tested against the shape the publisher actually emits -- including the
trap that broke the first implementation: the phrase "Schedule B" appearing
inside Schedule A's own instructional preamble, BEFORE the data table.
"""

from __future__ import annotations

import pytest

SCHEDULE_A_TEXT = """Schedule A
Direct Owners and Executive Officers
1. Complete Schedule A only if you are submitting an initial application or report. Schedule A asks for information about your direct owners and
executive officers. Use Schedule C to amend this information.
2. Direct Owners and Executive Officers. List below the names of:
(a) each Chief Executive Officer, Chief Financial Officer, Chief Operations Officer, Chief Legal Officer, Chief Compliance Officer, director, and any
other individuals with similar status or functions;
(e) if you are organized as a limited liability company ("LLC"), those members that have the right to receive upon dissolution.
3. Do you have any indirect owners to be reported on Schedule B? Yes No
FULL LEGAL NAME (Individuals: Last
Name, First Name, Middle Name)
DE/FE/ITitle or Status Date Title or Status
Acquired MM/YYYY
Ownership
Code
Control
Person
PR CRD No. If None: S.S. No. and Date
of Birth, IRS Tax No. or Employer
ID No.
TERZO, JUSTIN, MICHAEL I MANAGING PARTNER,
CHIEF INVESTMENT
OFFICER
04/2018 E Y N 5716648
STOCK, JOSEPH, DAVID I MANAGING PARTNER,
CHIEF COMPLIANCE
OFFICER
04/2018 B Y N 5816404
Schedule B
Indirect Owners
1. Complete Schedule B only if you are submitting an initial application or report.
No Information Filed
Schedule D
"""

SECTION_4_EMPTY = """SECTION 4 Successions
No Information Filed
Item 5 Information About Your Advisory Business
"""

SECTION_4_FILED = """SECTION 4 Successions
Name of Acquired Firm: EXAMPLE LEGACY ADVISORS LLC
CRD Number of Acquired Firm: 111111
Date of Succession: 03/14/2026
Item 5 Information About Your Advisory Business
"""


@pytest.fixture
def sample_schedule_a_text() -> str:
    return SCHEDULE_A_TEXT


@pytest.fixture
def section4_empty() -> str:
    return SECTION_4_EMPTY


@pytest.fixture
def section4_filed() -> str:
    return SECTION_4_FILED


@pytest.fixture(autouse=True)
def isolated_state(monkeypatch, tmp_path):
    """No test may touch a real state home or a real ledger."""
    monkeypatch.setenv("FDU_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv("FDU_DB_PATH", str(tmp_path / "state" / "test.sqlite3"))
    monkeypatch.delenv("FDU_HALT", raising=False)
