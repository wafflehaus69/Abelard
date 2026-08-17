"""B5 acceptance tests for staleness detection and the fallback path."""
from capex_daemon import edgar, facts_api, freshness, ixbrl

CONCEPT = "PaymentsToAcquirePropertyPlantAndEquipment"


def submissions(*rows):
    """rows: (form, filingDate, reportDate, accession, primaryDocument)"""
    cols = list(zip(*rows)) if rows else [[]] * 5
    return {"filings": {"recent": {
        "form": list(cols[0]), "filingDate": list(cols[1]),
        "reportDate": list(cols[2]), "accessionNumber": list(cols[3]),
        "primaryDocument": list(cols[4])}}}


def api_index(*period_ends):
    rows = [facts_api.ApiFact(CONCEPT, "us-gaap", "USD", 1.0, None, e, None,
                              "10-Q", "2026-01-01", None) for e in period_ends]
    return {CONCEPT: rows}


META_Q2 = ("10-Q", "2026-07-30", "2026-06-30", "0001628280-26-050705", "meta-20260630.htm")


def test_the_meta_gap_is_detected_as_stale():
    """Filed 2026-07-30 for period 2026-06-30; companyfacts stopped at 2026-03-31."""
    s = freshness.assess("0001326801", submissions(META_Q2),
                         api_index("2026-03-31"), CONCEPT)
    assert s.status == freshness.STATUS_STALE
    assert s.needs_fallback
    assert s.api_latest == "2026-03-31"
    assert "2026-06-30" in s.detail


def test_current_when_the_api_has_caught_up():
    """Re-checked 2026-08-13: the same filing is now served."""
    s = freshness.assess("0001326801", submissions(META_Q2),
                         api_index("2026-03-31", "2026-06-30"), CONCEPT)
    assert s.status == freshness.STATUS_CURRENT
    assert not s.needs_fallback


def test_empty_api_coverage_is_stale_not_current():
    s = freshness.assess("0001181412", submissions(META_Q2), {}, CONCEPT)
    assert s.status == freshness.STATUS_STALE
    assert s.api_latest is None


def test_no_periodic_filings_is_its_own_status():
    """An FPI files 20-F/6-K; absence of 10-K/10-Q is not staleness."""
    s = freshness.assess("0001513845",
                         submissions(("6-K", "2026-07-27", "", "0001104659-26-087080", "x.htm")),
                         {}, CONCEPT)
    assert s.status == freshness.STATUS_NO_FILINGS
    assert not s.needs_fallback


def test_latest_filing_is_chosen_by_filing_date():
    s = freshness.latest_periodic_filing(submissions(
        ("10-Q", "2026-04-30", "2026-03-31", "acc-old", "a.htm"),
        META_Q2,
        ("8-K", "2026-08-01", "2026-08-01", "acc-8k", "b.htm"),
    ))
    assert s.accession == "0001628280-26-050705"
    assert s.form == "10-Q"


# --- instance derivation --------------------------------------------------

def test_instance_document_name_derivation():
    assert edgar.instance_document_name("meta-20260630.htm") == "meta-20260630_htm.xml"
    assert edgar.instance_document_name("msft-20260630.htm") == "msft-20260630_htm.xml"


def test_no_instance_for_a_non_html_primary_document():
    """A filing without an iXBRL page has no extracted instance to fall back to,
    and the caller must be told rather than handed a guessed filename."""
    assert edgar.instance_document_name("primary_doc.xml") is None
    assert edgar.instance_document_name(None) is None


# --- G1 on the fallback path ----------------------------------------------

def fact(basis, value=1.0):
    return ixbrl.Fact("us-gaap", CONCEPT, value, "USD", None, basis,
                      None, "2026-06-30", {}, "c1")


def test_g1_gate_separates_undetermined_basis_facts():
    """The fallback path inherits a scale hazard the API path lacks (E5)."""
    ok, undetermined = freshness.gate_scale_basis([
        fact(ixbrl.SCALE_BASIS_INSTANCE),
        fact(ixbrl.SCALE_BASIS_IXBRL),
        fact(ixbrl.SCALE_BASIS_UNDETERMINED),
    ])
    assert len(ok) == 2
    assert len(undetermined) == 1
    assert undetermined[0].scale_basis == ixbrl.SCALE_BASIS_UNDETERMINED


def test_api_facts_carry_the_absolute_basis():
    assert facts_api.SCALE_BASIS_API == "api-absolute"
    assert freshness.provenance_for(facts_api.SCALE_BASIS_API) == freshness.PROVENANCE_API


def test_provenance_values_are_distinct():
    assert freshness.PROVENANCE_API != freshness.PROVENANCE_INSTANCE
