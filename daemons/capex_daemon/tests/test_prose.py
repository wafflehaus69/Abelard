"""CD-GAP1 P3 acceptance: prose-derived capex, admitted 2026-08-26.

Fixtures are the real sentences from Nebius's own exhibits, not invented ones —
the boilerplate case in particular has to be the actual boilerplate, because
that is the string the naive extractor matched.
"""
from capex_daemon import disclosure, prose, trend

REAL_Q1 = (" information about our capital expenditures: Three months ended "
           "March 31, 2025 2026 (in millions of U.S. dollars) Purchases of "
           "property and equipment and intangible assets (543.9) (2,472.9) ")
REAL_H1 = (" information about our capital expenditures: Six months ended "
           "June 30, 2025 2026 (in millions of U.S. dollars) Purchases of "
           "property and equipment and intangible assets (1,054.5) (8,130.3) ")
BOILERPLATE = (" our future financial and business performance, strategy, "
               "expected growth, planned investments and capital expenditures, "
               "capacity expansion plans, anticipated future financing ")


def test_the_boilerplate_yields_nothing():
    """It appears in nearly every release. A phrase-keyed extractor takes all of
    them and returns no data at all."""
    assert prose.parse_table(BOILERPLATE) == []
    assert "capital expenditures" in BOILERPLATE      # the trap is really there


def test_the_real_tables_parse_to_their_reported_periods():
    assert prose.parse_table(REAL_Q1) == [(1, 1, 2025, 543.9), (1, 1, 2026, 2472.9)]
    assert prose.parse_table(REAL_H1) == [(2, 2, 2025, 1054.5), (2, 2, 2026, 8130.3)]


def test_cumulative_periods_difference_into_quarters():
    """Same operation the XBRL normalizer performs, applied to prose."""
    rows = prose.parse_table(REAL_Q1) + prose.parse_table(REAL_H1)
    q = prose.discrete_from_cumulative(rows)
    assert q["2026Q1"] == 2472.9e6
    assert round(q["2026Q2"] / 1e6, 1) == 5657.4        # 8,130.3 - 2,472.9
    assert round(q["2025Q2"] / 1e6, 1) == 510.6         # 1,054.5 - 543.9


def test_the_series_can_never_be_contiguous_and_never_classifies():
    """Nebius reports Q1, H1 and FY, so Q3 is never separate and Q4 is only ever
    inside an H2 lump. No TTM, no TTM YoY, no phase state — ever, from prose."""
    rows = prose.parse_table(REAL_Q1) + prose.parse_table(REAL_H1)
    q = prose.discrete_from_cumulative(rows)
    assert set(q) == {"2025Q1", "2025Q2", "2026Q1", "2026Q2"}
    assert trend.ttm_by_quarter(q) == {}                 # the gap forbids a TTM
    assert trend.issuer_yoy(q) == {}


def test_the_honest_growth_read_is_half_over_half():
    rows = prose.parse_table(REAL_Q1) + prose.parse_table(REAL_H1)
    g = prose.half_over_half(rows)
    assert round(100 * g) == 671                         # 8,130.3 / 1,054.5 - 1


def test_the_basis_is_marked_because_it_is_broader_than_the_panel():
    """Intangibles are not PP&E (E23). The marker is what stops a broader
    measure joining a series that assumes it is not."""
    spec = prose.source_for("0001513845")
    assert spec["basis"] == prose.BASIS_PPE_PLUS_INTANGIBLES
    assert "intangible" in spec["line_label"].lower()
    assert spec["ruled_on"] == "2026-08-26"
    assert disclosure.CAUSE_FROM_PROSE == "DERIVED-FROM-PROSE"


def test_prose_reading_is_restricted_to_declared_issuers():
    """Not a general prose reader, and must not become one by accident."""
    assert prose.source_for("0000789019") is None        # MSFT
    assert prose.source_for(None) is None
    assert list(prose.PROSE_SOURCES) == ["0001513845"]


def test_the_anchors_that_verified_it_are_recorded():
    """The ordered gate: extracted figures checked against known annuals."""
    a = prose.source_for("0001513845")["anchors"]
    assert a == {"2022": 14.6, "2023": 83.4, "2024": 807.7}
    # H1-2025 alone exceeds all of FY2024, consistent with the release language.
    assert 1054.5 > a["2024"]
