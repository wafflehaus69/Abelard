"""CD-2 acceptance tests: fee-exhibit ingest, commitments, composition, divergence."""
from capex_daemon import charts, commitments, divergence, feeexhibit, ixbrl

# A structurally faithful miniature of a real EX-FILING FEES exhibit: typed
# OfferingAxis dimension, two debt tranches, tranche sum != stated total.
FEE_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<xbrl xmlns="http://www.xbrl.org/2003/instance"
      xmlns:dei="http://xbrl.sec.gov/dei/2026"
      xmlns:ffd="http://xbrl.sec.gov/ffd/2026"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi">
  <context id="rc"><entity><identifier scheme="http://www.sec.gov/CIK">0001326801</identifier></entity>
    <period><startDate>2026-05-01</startDate><endDate>2026-05-01</endDate></period></context>
  <context id="o1"><entity><identifier scheme="http://www.sec.gov/CIK">0001326801</identifier>
    <segment><xbrldi:typedMember dimension="ffd:OfferingAxis"><dei:lineNo>1</dei:lineNo></xbrldi:typedMember></segment>
    </entity><period><startDate>2026-05-01</startDate><endDate>2026-05-01</endDate></period></context>
  <context id="o2"><entity><identifier scheme="http://www.sec.gov/CIK">0001326801</identifier>
    <segment><xbrldi:typedMember dimension="ffd:OfferingAxis"><dei:lineNo>2</dei:lineNo></xbrldi:typedMember></segment>
    </entity><period><startDate>2026-05-01</startDate><endDate>2026-05-01</endDate></period></context>
  <unit id="U"><measure>iso4217:USD</measure></unit>
  <ffd:OfferingSctyTp contextRef="o1">Debt</ffd:OfferingSctyTp>
  <ffd:AmtSctiesRegd contextRef="o1" unitRef="U" decimals="0">3000000000</ffd:AmtSctiesRegd>
  <ffd:OfferingSctyTp contextRef="o2">Debt</ffd:OfferingSctyTp>
  <ffd:AmtSctiesRegd contextRef="o2" unitRef="U" decimals="0">2000000000</ffd:AmtSctiesRegd>
  <ffd:TtlOfferingAmt contextRef="rc" unitRef="U" decimals="2">4993478000.00</ffd:TtlOfferingAmt>
  <ffd:NetFeeAmt contextRef="rc" unitRef="U" decimals="2">690335.66</ffd:NetFeeAmt>
  <ffd:RegnFileNb contextRef="rc">333-295425</ffd:RegnFileNb>
  <ffd:SubmssnTp contextRef="rc">424B2</ffd:SubmssnTp>
  <ffd:FnlPrspctsFlg contextRef="rc">true</ffd:FnlPrspctsFlg>
</xbrl>
"""

PRELIM_XML = FEE_XML.replace(b"<ffd:FnlPrspctsFlg contextRef=\"rc\">true",
                             b"<ffd:FnlPrspctsFlg contextRef=\"rc\">false")


# --- C1: typed dimensions and tranche discrimination ---------------------

def test_typed_dimension_discriminates_tranches():
    """ffd:OfferingAxis is a TYPED dimension; reading only explicitMember would
    collapse every tranche of a multi-tranche offering into one context."""
    facts = ixbrl.parse_instance(FEE_XML)
    lines = {f.dims.get("OfferingAxis") for f in facts if f.dims}
    assert lines == {"1", "2"}


def test_fee_exhibit_parses_debt_tranches():
    off = feeexhibit.parse_fee_exhibit(FEE_XML, accession="acc-1")
    assert len(off.debt_tranches) == 2
    assert off.debt_principal == 5_000_000_000
    assert off.is_final is True
    assert off.registration_file == "333-295425"


def test_tranche_sum_and_stated_total_are_both_reported():
    """The gap is offering discount and is structural. Collapsing it would
    invent precision; Meta's real gap is $32.61M, Oracle's $38.2M."""
    off = feeexhibit.parse_fee_exhibit(FEE_XML)
    s, total, diff = off.tranche_sum_vs_total
    assert s == 5_000_000_000
    assert total == 4_993_478_000.0
    assert round(diff, 2) == 6_522_000.00


def test_preliminary_prospectus_is_flagged_not_final():
    """A 'SUBJECT TO COMPLETION' prospectus prints blank amounts; FnlPrspctsFlg
    is the discriminator and a preliminary is never counted as an event."""
    off = feeexhibit.parse_fee_exhibit(PRELIM_XML)
    assert off.is_final is False


def test_dedup_key_is_content_derived_not_time_derived():
    off = feeexhibit.parse_fee_exhibit(FEE_XML, cik="0001326801", accession="acc-1")
    assert off.dedup_key == "0001326801:acc-1"


def test_watermark_advances_only_on_success_with_items():
    off = feeexhibit.parse_fee_exhibit(FEE_XML, accession="a")
    off.filed = "2026-05-01"
    assert feeexhibit.advance_watermark("2026-04-01", [off]) == "2026-05-01"
    assert feeexhibit.advance_watermark("2026-04-01", []) == "2026-04-01"
    # never moves backwards
    assert feeexhibit.advance_watermark("2026-06-01", [off]) == "2026-06-01"


def test_candidate_discovery_does_not_filter_on_8k_item_codes():
    """Meta's debt 8-K was filed under items 8.01/9.01, not 1.01/2.03."""
    subs = {"filings": {"recent": {
        "form": ["8-K", "424B2"], "filingDate": ["2026-05-04", "2026-05-01"],
        "accessionNumber": ["a1", "a2"], "primaryDocument": ["d1.htm", "d2.htm"]}}}
    got = feeexhibit.candidate_filings(subs)
    assert {r["accession"] for r in got} == {"a1", "a2"}


# --- C2/C3: commitments and deposits -------------------------------------

def test_untagged_disclosure_publishes_status_never_zero():
    """MSFT's $194.06B table is disclosed-dark by ruling."""
    s = commitments.forward_commitments("0000789019", {})
    assert s.status == commitments.STATUS_UNCOVERED_UNTAGGED
    assert s.points == []
    assert "194.06B" in s.detail


def test_absent_commitments_are_distinct_from_untagged():
    s = commitments.forward_commitments("0000000001", {})
    assert s.status == commitments.STATUS_ABSENT


def test_deposits_series_requires_a_verified_line_mapping():
    """RIOT's concept means deposits; HUT's identical concept means purchases.
    Absence of a mapping must not be read as absence of the concept (E23)."""
    assert "0001167419" in commitments.DEPOSIT_LINE_MAPPING
    assert commitments.equipment_deposits("0001964789", {}) is None  # HUT: no mapping


# --- C4/C5: composition and divergence -----------------------------------

class V:
    def __init__(self, ticker, bucket, capex, issuance=None, statuses=("OK",)):
        self.ticker, self.bucket = ticker, bucket
        self.ttm_capex, self.ttm_issuance = capex, issuance
        self.ratio = (issuance / capex) if (issuance and capex) else None
        self.statuses = list(statuses)
        self.commitments = None


def test_concentration_is_disclosed_with_every_subtotal():
    views = [V("A", "builder", 16.6e9), V("B", "builder", 0.3e9), V("C", "builder", 0.2e9)]
    comp = divergence.composition(views)
    b = comp["buckets"]["builder"]
    assert b["n"] == 3
    assert 0.97 < b["top2_share"] < 0.99          # top-2 dominate a 3-name bucket
    assert comp["total"] == b["subtotal"]


def test_excluded_members_are_named_not_dropped():
    views = [V("A", "hyperscaler", 100e9), V("B", "builder", None, statuses=("CAPEX-UNRESOLVED",))]
    comp = divergence.composition(views)
    assert ("B", ["CAPEX-UNRESOLVED"]) in comp["excluded"]


def test_bucket_ratio_names_issuers_excluded_from_it():
    """A ratio computed over a partial denominator must say so (E16)."""
    views = [V("A", "hyperscaler", 100e9, 50e9), V("B", "hyperscaler", 100e9, None)]
    out = divergence.bucket_divergence(views)["hyperscaler"]
    assert out["ratio"] == 0.5
    assert out["excluded_no_issuance"] == ["B"]
    assert out["contributing"] == ["A"]


def test_negative_issuance_status_exists_and_is_distinct():
    """A summed stack that nets negative is not gross issuance; WULF nets -$0.88B."""
    assert divergence.STATUS_ISSUANCE_NET_NEGATIVE != divergence.STATUS_ISSUANCE_REFUSED
    assert divergence.STATUS_ISSUANCE_NO_OVERLAP != divergence.STATUS_ISSUANCE_REFUSED


def test_divergence_rows_emit_none_not_zero_for_withheld_ratios():
    views = [V("A", "builder", 10e9, None, statuses=("ISSUANCE-REFUSED",))]
    row = divergence.divergence_rows(views)[0]
    assert row[4] is None
    assert "ISSUANCE-REFUSED" in row[5]


def test_bucket_colors_cover_every_published_bucket():
    for b in divergence.BUCKET_ORDER:
        assert b in charts.BUCKET_COLORS
