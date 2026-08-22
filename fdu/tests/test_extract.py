"""Extraction tests against the shapes the publisher actually emits."""

from __future__ import annotations

import pytest

from fdu_daemon.adv_pdf import (
    _owner_tables,
    _parse_owner_block,
    _section,
    run_tail_pages,
)


def test_schedule_a_table_found_despite_schedule_b_in_preamble(sample_schedule_a_text):
    """The bug this fixture exists to catch.

    "Schedule B" appears in Schedule A's instructions, before the data table. A
    heading-to-heading slice ends there and reports zero owners for a firm with
    two.
    """
    tables = _owner_tables(sample_schedule_a_text)
    assert tables["A"], "Schedule A table not located"
    count, codes, controls, acquired = _parse_owner_block(tables["A"])
    assert count == 2
    assert sorted(codes) == ["B", "E"]


def test_indirect_owners_classified_separately(sample_schedule_a_text):
    tables = _owner_tables(sample_schedule_a_text)
    # The fixture's Schedule B is "No Information Filed" -- no table header, so
    # no rows should be attributed to it.
    count, _c, _ct, _a = _parse_owner_block(tables["B"])
    assert count == 0


def test_ownership_codes_ordering():
    from fdu_daemon.adv_pdf import OWNERSHIP_CODE_ORDER

    assert OWNERSHIP_CODE_ORDER.index("F") > OWNERSHIP_CODE_ORDER.index("A")
    assert OWNERSHIP_CODE_ORDER.index("A") > OWNERSHIP_CODE_ORDER.index("NA")


def test_section4_empty_detected(section4_empty):
    seg = _section(section4_empty, "SECTION 4 Successions", "Item 5")
    body = seg[len("SECTION 4 Successions"):].strip()
    assert "No Information Filed" in body[:80]


def test_section4_filed_detected(section4_filed):
    seg = _section(section4_filed, "SECTION 4 Successions", "Item 5")
    body = seg[len("SECTION 4 Successions"):].strip()
    assert "No Information Filed" not in body[:80]
    assert "EXAMPLE LEGACY ADVISORS" in body


def test_section_respects_earliest_end_marker():
    text = "SECTION 4 Successions AAA SECTION 5 BBB Item 5 CCC"
    seg = _section(text, "SECTION 4 Successions", "Item 5", "SECTION 5")
    assert "BBB" not in seg


# -- run-tail detection --------------------------------------------------


def test_run_tails_single_run():
    sizes = [212] * 24
    assert run_tail_pages(sizes) == [23]


def test_run_tails_multi_run():
    """Measured shape: a 68-page filing spikes at pages 24 and 48."""
    sizes = [212] * 68
    sizes[24] = 377
    sizes[48] = 377
    tails = run_tail_pages(sizes)
    assert set(tails) == {23, 47, 67}
    assert tails[0] == 23, "the base Part 1A run must be read first"
    assert tails[1] == 67, "the final run must be read second"


def test_run_tails_empty():
    assert run_tail_pages([]) == []


def test_run_tails_all_unreadable():
    assert run_tail_pages([-1, -1, -1]) == [2]


@pytest.mark.parametrize("n_spikes", [1, 2, 31])
def test_run_tails_never_duplicates(n_spikes):
    sizes = [212] * (24 * (n_spikes + 1))
    for i in range(1, n_spikes + 1):
        sizes[24 * i] = 377
    tails = run_tail_pages(sizes)
    assert len(tails) == len(set(tails))
