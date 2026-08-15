"""B6 acceptance tests for the R-B6-1 collapse/sum/refuse rule and normalization."""
from capex_daemon import issuance, normalize


def s(**periods):
    """s(y2025=100) -> {('2025-01-01', '2025-12-31'): 100}"""
    return {(f"{k[1:]}-01-01", f"{k[1:]}-12-31"): v for k, v in periods.items()}


# --- rule (a): collapse double-tags --------------------------------------

def test_identical_values_across_all_shared_periods_collapse():
    a = s(y2024=1_000, y2025=2_000)
    b = s(y2024=1_000, y2025=2_000)
    branch, shared, _ = issuance.classify_pair(a, b)
    assert branch == issuance.BRANCH_COLLAPSED
    assert len(shared) == 2


def test_collapse_tolerates_last_digit_rounding():
    branch, _, _ = issuance.classify_pair(s(y2025=1_000_000), s(y2025=1_000_001))
    assert branch == issuance.BRANCH_COLLAPSED


# --- rule (b): sum disjoint instruments ----------------------------------

def test_no_shared_periods_is_disjoint_by_construction():
    branch, shared, _ = issuance.classify_pair(s(y2020=5), s(y2025=7))
    assert branch == issuance.BRANCH_SUMMED
    assert shared == []


def test_values_differing_in_both_directions_are_distinct_instruments():
    a = s(y2024=100, y2025=50)
    b = s(y2024=50, y2025=100)
    branch, _, _ = issuance.classify_pair(a, b)
    assert branch == issuance.BRANCH_SUMMED


# --- rule (c): refuse containment ----------------------------------------

def test_persistent_containment_refuses_rather_than_summing():
    """A child never exceeding a parent may be a subset; summing double-counts."""
    parent = s(y2023=1_000, y2024=2_000, y2025=3_000)
    child = s(y2023=100, y2024=200, y2025=300)
    branch, shared, detail = issuance.classify_pair(child, parent)
    assert branch == issuance.BRANCH_REFUSED
    assert len(shared) == 3
    assert "subset" in detail


def test_a_single_shared_period_is_not_enough_to_refuse():
    """One period of containment is coincidence, not evidence."""
    branch, _, _ = issuance.classify_pair(s(y2025=100), s(y2025=1_000))
    assert branch != issuance.BRANCH_REFUSED


# --- derived totals -------------------------------------------------------

def test_refusal_yields_no_total_and_never_zero():
    res = issuance.IssuanceResolution(issuance.STATUS_REFUSED, (), (), (), "x")
    assert res.is_refused
    assert res.total_for({}, ("2025-01-01", "2025-12-31")) is None


def test_summed_total_adds_contributing_concepts_only():
    smap = {"A": s(y2025=100), "B": s(y2025=250), "C": s(y2025=999)}
    res = issuance.IssuanceResolution(issuance.STATUS_OK, ("A", "B"), ("C",), (), "x")
    assert res.total_for(smap, ("2025-01-01", "2025-12-31")) == 350


# --- normalization --------------------------------------------------------

def test_calendar_alignment_reports_offset_for_off_grid_filers():
    """SNOW closes Apr 30 and ORCL May 31; neither lands on a calendar quarter
    end, and the distance is carried on the row rather than rounded away."""
    assert normalize.calendar_align("2026-04-30") == ("2026Q2", -61)   # SNOW
    assert normalize.calendar_align("2026-05-31") == ("2026Q2", -30)   # ORCL
    assert normalize.calendar_align("2026-02-28") == ("2026Q1", -31)   # ORCL Q3


def test_calendar_alignment_is_exact_for_calendar_filers():
    for end in ("2026-03-31", "2026-06-30", "2026-09-30", "2026-12-31"):
        assert normalize.calendar_align(end)[1] == 0


def test_ttm_needs_a_full_window():
    class R:
        def __init__(self, v):
            self.value = v
    assert normalize.ttm([R(1), R(2), R(3)]) is None
    assert normalize.ttm([R(1), R(2), R(3), R(4)]) == 10
