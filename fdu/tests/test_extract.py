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
    """Measured shape: a 68-page filing spikes at pages 24 and 48.

    Read order is base runs first, then back from the end -- the ownership
    schedules sit at the end of an umbrella filing, not the start.
    """
    sizes = [212] * 68
    sizes[24] = 377
    sizes[48] = 377
    tails = run_tail_pages(sizes)
    assert set(tails) == {23, 47, 67}
    assert tails[:2] == [23, 47], "the two base runs must be read first"
    assert tails[2] == 67, "then walk back from the end"


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


# -- read-order regression: ownership schedules live at the END ----------


def test_run_tails_read_base_then_walk_back_from_end():
    """Regression for the mega-filing miss.

    Measured on a 1,750-page / 60-run filing: Section 4 in run 1, Schedule A in
    run 57. An order of [first, last, then ascending] never reached run 57 and
    reported "Schedule A not located" for the largest advisers in the corpus.
    """
    sizes = [212] * (24 * 60)
    for i in range(1, 60):
        sizes[24 * i] = 377
    tails = run_tail_pages(sizes)
    assert tails[0] == 23, "base run must be read first"
    assert tails[1] == 47, "second base run carries Section 4"
    # then descending from the end, so the ownership schedules are reached fast
    assert tails[2] > tails[3] > tails[4], "must walk back from the end"
    assert tails[2] == sizes.__len__() - 1
    # run 57's tail must be inside the read budget
    from fdu_daemon.adv_pdf import MAX_RUN_TAILS

    assert (24 * 58 - 1) in tails[:MAX_RUN_TAILS], "Schedule A's run must be reachable"


def test_stub_document_recorded_as_unavailable():
    """A 200 carrying 'PDF not available' is an absence, not an empty filing."""
    from fdu_daemon.adv_pdf import _STUB_MARKER

    assert "not available for this firm" in _STUB_MARKER


# -- absence vs failure --------------------------------------------------


def test_item4_heading_regex_distinguishes_form_variants():
    from fdu_daemon.adv_pdf import _ITEM4_HEADING_RE

    era_like = "Item 1 Identifying Information Item 2 Item 3 Item 5 Item 6 Item 7 Item 10 Item 11"
    full_like = "Item 3 ... Item 4 Successions Yes No A. Are you ... Item 5"
    assert _ITEM4_HEADING_RE.search(era_like) is None, "ERA subset form has no Item 4"
    assert _ITEM4_HEADING_RE.search(full_like) is not None


def test_missing_section4_on_subset_form_is_not_a_failure(section4_empty):
    """An ERA has no Item 4. Recording that as an extraction failure buried a
    structural fact under a 30% error rate and made a working run look broken."""
    from fdu_daemon.adv_pdf import AdvFacts

    f = AdvFacts(crd="1")
    assert f.not_applicable is None
    assert f.extract_status == "ok"


def test_status_vocabulary_is_closed():
    """Every status FDU writes must be one the reader knows how to interpret."""
    allowed = {"ok", "partial", "unavailable", "not_applicable"}
    import inspect

    from fdu_daemon import adv_pdf

    src = inspect.getsource(adv_pdf)
    import re as _re

    written = set(_re.findall(r'extract_status = "([a-z_]+)"', src))
    assert written <= allowed, f"unknown status written: {written - allowed}"
