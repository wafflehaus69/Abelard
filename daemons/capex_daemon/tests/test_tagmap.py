"""B3 acceptance tests for per-issuer, per-era concept resolution (E7)."""
from capex_daemon import facts_api, tagmap


def fact(concept, start, end, value, unit="USD", filed="2026-01-01"):
    return facts_api.ApiFact(concept, "us-gaap", unit, value, start, end,
                             None, "10-Q", filed, None)


def index(*facts):
    out = {}
    for f in facts:
        out.setdefault(f.concept, []).append(f)
    return out


# --- era resolution -------------------------------------------------------

def test_amazon_shaped_migration_resolves_to_the_newer_tag():
    """The abandoned 2017 tag must never own the present (E7)."""
    idx = index(
        fact("PaymentsToAcquirePropertyPlantAndEquipment", "2015-01-01", "2015-12-31", 1e9),
        fact("PaymentsToAcquirePropertyPlantAndEquipment", "2016-01-01", "2016-12-31", 2e9),
        fact("PaymentsToAcquireProductiveAssets", "2016-01-01", "2016-12-31", 2e9),
        fact("PaymentsToAcquireProductiveAssets", "2026-01-01", "2026-06-30", 9e10),
    )
    r = tagmap.resolve(idx, tagmap.CAPEX)
    assert r.current_concept == "PaymentsToAcquireProductiveAssets"
    assert r.concept_for("2026-06-30") == "PaymentsToAcquireProductiveAssets"


def test_older_era_still_owns_its_own_periods():
    idx = index(
        fact("PaymentsToAcquirePropertyPlantAndEquipment", "2015-01-01", "2015-12-31", 1e9),
        fact("PaymentsToAcquireProductiveAssets", "2020-01-01", "2020-12-31", 5e9),
    )
    r = tagmap.resolve(idx, tagmap.CAPEX)
    assert r.concept_for("2015-12-31") == "PaymentsToAcquirePropertyPlantAndEquipment"
    assert r.concept_for("2020-12-31") == "PaymentsToAcquireProductiveAssets"


def test_series_facts_drops_periods_owned_by_another_era():
    """The whole point of the era map: the stale tag's recent rows never surface."""
    idx = index(
        fact("PaymentsToAcquirePropertyPlantAndEquipment", "2018-01-01", "2018-12-31", 7_420_000_000),
        fact("PaymentsToAcquireProductiveAssets", "2026-01-01", "2026-12-31", 173_028_000_000),
    )
    r = tagmap.resolve(idx, tagmap.CAPEX)
    rows = tagmap.series_facts(idx, r)
    assert (173_028_000_000, "2026-12-31") in [(v.value, v.period_end) for v, _ in rows]
    assert r.concept_for("2026-12-31") == "PaymentsToAcquireProductiveAssets"


def test_disagreeing_concepts_on_one_period_refuse_rather_than_coin_flip():
    """Equal spans and equal counts leave recency no signal. Picking either
    would be a coin flip presented as an answer, so it refuses (E1)."""
    idx = index(
        fact("PaymentsToAcquirePropertyPlantAndEquipment", "2020-01-01", "2020-12-31", 7_420_000_000),
        fact("PaymentsToAcquireProductiveAssets", "2020-01-01", "2020-12-31", 173_028_000_000),
    )
    r = tagmap.resolve(idx, tagmap.CAPEX)
    assert r.is_multi_line
    assert r.current_concept == tagmap.UNRESOLVED_MULTILINE


def test_agreeing_concepts_on_one_period_are_harmless_redundancy():
    """EQIX double-tags the same capex value under two concepts in 67 periods.
    Same number, no ambiguity, no refusal."""
    idx = index(
        fact("PaymentsToAcquireOtherPropertyPlantAndEquipment", "2026-01-01", "2026-03-31", 2_834_000_000),
        fact("PaymentsToAcquireProductiveAssets", "2026-01-01", "2026-03-31", 2_834_000_000),
    )
    r = tagmap.resolve(idx, tagmap.CAPEX)
    assert not r.is_multi_line
    assert r.current_concept in (
        "PaymentsToAcquireOtherPropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets")


def test_unresolved_when_no_candidate_present():
    r = tagmap.resolve({}, tagmap.CAPEX)
    assert r.is_unresolved
    assert r.current_concept == tagmap.UNRESOLVED
    assert "no candidate" in r.why()


# --- multi-line refusal ---------------------------------------------------

def test_debt_stack_refuses_to_select():
    """WULF-shaped: several instruments live at once. Selecting one undercounts
    (measured 34x); summing is unruled. So it refuses and names them (E1, E8)."""
    idx = index(
        fact("ProceedsFromIssuanceOfSecuredDebt", "2025-01-01", "2025-12-31", 3_132_938_000),
        fact("ProceedsFromConvertibleDebt", "2025-01-01", "2025-12-31", 975_329_000),
        fact("ProceedsFromShortTermDebt", "2026-01-01", "2026-06-30", 92_750_000),
    )
    r = tagmap.resolve(idx, tagmap.DEBT)
    assert r.is_multi_line
    assert r.current_concept == tagmap.UNRESOLVED_MULTILINE
    assert "ProceedsFromIssuanceOfSecuredDebt" in r.multi_line_concepts
    assert "co-report" in r.why()


def test_a_long_dead_candidate_does_not_create_a_false_stack():
    """MSFT-shaped: a concept last tagged in 2010 is not part of the live stack."""
    idx = index(
        fact("ProceedsFromConvertibleDebt", "2010-01-01", "2010-06-30", 1e9),
        fact("ProceedsFromDebtMaturingInMoreThanThreeMonths", "2026-01-01", "2026-06-30", 0),
    )
    r = tagmap.resolve(idx, tagmap.DEBT)
    assert not r.is_multi_line
    assert r.current_concept == "ProceedsFromDebtMaturingInMoreThanThreeMonths"


def test_capex_is_never_treated_as_multi_line():
    """Capex is one migrating cash-flow line; only DEBT is a stack."""
    assert tagmap.CAPEX not in tagmap.MULTI_LINE_KINDS
    assert tagmap.ANCHOR not in tagmap.MULTI_LINE_KINDS
    assert tagmap.DEBT in tagmap.MULTI_LINE_KINDS


# --- anchor map -----------------------------------------------------------

def test_anchor_candidates_are_gross_basis_only():
    """Net cannot anchor capex — depreciation confounds the delta. An issuer
    with no gross concept is UNANCHORED, never approximated from net."""
    assert "PropertyPlantAndEquipmentNet" not in tagmap.CANDIDATES[tagmap.ANCHOR]
    assert "PropertyPlantAndEquipmentGross" in tagmap.CANDIDATES[tagmap.ANCHOR]


def test_meta_shaped_anchor_prefers_the_combined_concept():
    """R2a: Meta's anchor bundles finance-lease ROU, so capex alone cannot reconcile."""
    combined = ("PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAsset"
                "BeforeAccumulatedDepreciationAndAmortization")
    idx = index(
        facts_api.ApiFact("PropertyPlantAndEquipmentGross", "us-gaap", "USD",
                          1e9, None, "2019-12-31", None, "10-K", "2020-01-01", None),
        facts_api.ApiFact(combined, "us-gaap", "USD",
                          256e9, None, "2026-03-31", None, "10-Q", "2026-04-30", None),
    )
    r = tagmap.resolve(idx, tagmap.ANCHOR)
    assert r.current_concept == combined


def test_reit_anchor_resolves_to_the_real_estate_concept():
    idx = index(
        facts_api.ApiFact("RealEstateInvestmentPropertyAtCost", "us-gaap", "USD",
                          31.6e9, None, "2026-03-31", None, "10-Q", "2026-04-30", None),
    )
    r = tagmap.resolve(idx, tagmap.ANCHOR)
    assert r.current_concept == "RealEstateInvestmentPropertyAtCost"


# --- provenance -----------------------------------------------------------

def test_every_series_row_carries_its_resolved_concept():
    idx = index(fact("PaymentsToAcquireProductiveAssets", "2026-01-01", "2026-03-31", 44e9))
    r = tagmap.resolve(idx, tagmap.CAPEX)
    rows = tagmap.series_facts(idx, r)
    assert rows
    assert all(concept for _, concept in rows)


def test_restatements_collapse_to_the_latest_filed():
    idx = index(
        fact("PaymentsToAcquireProductiveAssets", "2026-01-01", "2026-03-31", 44e9, filed="2026-04-30"),
        fact("PaymentsToAcquireProductiveAssets", "2026-01-01", "2026-03-31", 45e9, filed="2026-07-31"),
    )
    r = tagmap.resolve(idx, tagmap.CAPEX)
    rows = tagmap.series_facts(idx, r)
    assert len(rows) == 1
    assert rows[0][0].value == 45e9


# --- ruled concept elections (CD-GAP1 P2) ----------------------------------

def _riot_like():
    """RIOT's real shape and real numbers, from the 2026Q1 10-Q cash-flow
    statement quoted in CD-1-VERIFY §1: two SEPARATE investing lines,
    co-reported, different values — PP&E purchases and equipment deposits."""
    return index(
        fact("PaymentsToAcquirePropertyPlantAndEquipment",
             "2026-01-01", "2026-03-31", 115465000.0),
        fact("PaymentsToAcquirePropertyPlantAndEquipment",
             "2025-01-01", "2025-03-31", 32858000.0),
        fact("PaymentsToAcquireMachineryAndEquipment",
             "2026-01-01", "2026-03-31", 16184000.0),
        fact("PaymentsToAcquireMachineryAndEquipment",
             "2025-01-01", "2025-03-31", 26655000.0),
    )


def test_without_a_ruling_the_ambiguity_still_refuses():
    """The guard is right by default: two concepts, one period, different
    values, and recency cannot arbitrate. Refusing stays the behaviour."""
    r = tagmap.resolve(_riot_like(), tagmap.CAPEX)
    assert r.is_multi_line
    assert r.current_concept == tagmap.UNRESOLVED_MULTILINE
    assert not r.is_ruled


def test_a_ruling_arbitrates_what_recency_cannot():
    """R-B6-4 read the filing and ruled. The guard defers to the ruling rather
    than overriding it — that is the whole point of having ruled."""
    r = tagmap.resolve(_riot_like(), tagmap.CAPEX, cik="0001167419")
    assert r.is_ruled and r.ruling["ruling"] == "R-B6-4"
    assert not r.is_multi_line
    assert r.current_concept == "PaymentsToAcquirePropertyPlantAndEquipment"


def test_the_excluded_line_cannot_re_enter_through_the_overlap_check():
    """Equipment deposits are dropped from the span map entirely, not merely
    out-ranked — summing them would double-count when the equipment lands."""
    r = tagmap.resolve(_riot_like(), tagmap.CAPEX, cik="0001167419")
    assert "PaymentsToAcquireMachineryAndEquipment" not in r.candidates_present
    assert "PaymentsToAcquireMachineryAndEquipment" not in r.multi_line_concepts


def test_a_ruled_election_is_marked_and_never_passes_as_automatic():
    """Same discipline as MU's unit mapping: a human judgement stays visible."""
    auto = tagmap.resolve(_riot_like(), tagmap.CAPEX)
    ruled = tagmap.resolve(_riot_like(), tagmap.CAPEX, cik="0001167419")
    assert auto.ruling is None and auto.is_ruled is False
    assert ruled.is_ruled and ruled.ruling["ruled_on"] == "2026-08-14"
    assert tagmap.STATUS_RULED == "RULED-CONCEPT"


def test_a_ruling_for_another_issuer_does_not_leak():
    assert tagmap.ruled_concept("0000789019", tagmap.CAPEX) is None
    assert tagmap.ruled_concept(None, tagmap.CAPEX) is None
    # ...and the RIOT ruling is scoped to CAPEX, not to every kind.
    assert tagmap.ruled_concept("0001167419", tagmap.DEBT) is None
