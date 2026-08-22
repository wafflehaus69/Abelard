"""B2 acceptance tests for the filing-level XBRL parser.

Fixtures are synthetic but structurally faithful to the real filings that
motivated each rule — the MSFT FY2026 10-K double-wrap and the Meta Q2-2026
instance. Real-filing verification lives in CD-1-VERIFY.md; these run offline.
"""
from capex_daemon import ixbrl

# Mirrors msft-20260630.htm: one displayed number, scale="9", wrapped by two
# ix:nonFraction elements whose contexts differ only in lease-term member.
MSFT_NESTED = b"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:us-gaap="http://fasb.org/us-gaap/2026"
      xmlns:msft="http://www.microsoft.com/20260630">
<body>
<div style="display:none">
<ix:header><ix:resources>
  <xbrli:context id="C_FIN">
    <xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0000789019</xbrli:identifier>
      <xbrli:segment><xbrldi:explicitMember dimension="us-gaap:LeaseContractualTermAxis">msft:FinanceLeaseMember</xbrldi:explicitMember></xbrli:segment>
    </xbrli:entity>
    <xbrli:period><xbrli:instant>2026-06-30</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:context id="C_OPS">
    <xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0000789019</xbrli:identifier>
      <xbrli:segment><xbrldi:explicitMember dimension="us-gaap:LeaseContractualTermAxis">msft:OperatingLeaseMember</xbrldi:explicitMember></xbrli:segment>
    </xbrli:entity>
    <xbrli:period><xbrli:instant>2026-06-30</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:unit id="U_USD"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
</ix:resources></ix:header>
</div>
<p>additional leases, primarily for datacenters, that had not yet commenced of $
<ix:nonFraction contextRef="C_FIN" name="us-gaap:UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount" unitRef="U_USD" scale="9" decimals="-8"><ix:nonFraction contextRef="C_OPS" name="us-gaap:UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount" unitRef="U_USD" scale="9" decimals="-8">329.1</ix:nonFraction></ix:nonFraction>
 billion</p>
</body></html>
"""

# Mirrors meta-20260630_htm.xml: same concept and period, one dimensioned to the
# Louisiana campus and one consolidated. companyfacts returns only the latter.
META_DUAL = b"""<?xml version="1.0" encoding="utf-8"?>
<xbrl xmlns="http://www.xbrl.org/2003/instance"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
      xmlns:us-gaap="http://fasb.org/us-gaap/2026"
      xmlns:meta="http://www.meta.com/20260630">
  <context id="C_LA">
    <entity><identifier scheme="http://www.sec.gov/CIK">0001326801</identifier>
      <segment><xbrldi:explicitMember dimension="us-gaap:ScheduleOfEquityMethodInvestmentEquityMethodInvesteeNameAxis">meta:DataCenterCampusInLouisianaMember</xbrldi:explicitMember></segment>
    </entity>
    <period><instant>2025-12-31</instant></period>
  </context>
  <context id="C_CONS">
    <entity><identifier scheme="http://www.sec.gov/CIK">0001326801</identifier></entity>
    <period><instant>2025-12-31</instant></period>
  </context>
  <unit id="U_USD"><measure>iso4217:USD</measure></unit>
  <us-gaap:VariableInterestEntityEntityMaximumLossExposureAmount contextRef="C_LA" unitRef="U_USD" decimals="-7">45950000000</us-gaap:VariableInterestEntityEntityMaximumLossExposureAmount>
  <us-gaap:VariableInterestEntityEntityMaximumLossExposureAmount contextRef="C_CONS" unitRef="U_USD" decimals="-7">5580000000</us-gaap:VariableInterestEntityEntityMaximumLossExposureAmount>
</xbrl>
"""


def test_ixbrl_scale_attribute_is_applied():
    """scale="9" on a displayed 329.1 means 329,100,000,000 (E5)."""
    facts = ixbrl.parse_ixbrl(MSFT_NESTED)
    hits = ixbrl.select(facts, concept="UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount")
    assert len(hits) == 1
    assert hits[0].value == 329_100_000_000
    assert hits[0].scale == 9
    assert hits[0].scale_basis == ixbrl.SCALE_BASIS_IXBRL


def test_nested_nonfraction_collapses_to_exactly_one():
    """The double-wrap must not become two facts — that overstates by 2x."""
    facts = ixbrl.parse_ixbrl(MSFT_NESTED)
    matching = [f for f in facts if f.value == 329_100_000_000]
    assert len(matching) == 1, "nested wrap produced {} facts".format(len(matching))


def test_collapsed_context_is_recorded_not_dropped():
    """The loss must be visible on the surviving row."""
    facts = ixbrl.parse_ixbrl(MSFT_NESTED)
    fact = ixbrl.select(facts, concept="UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount")[0]
    assert fact.collapsed_context_refs == ["C_OPS"]
    assert fact.dims == {"LeaseContractualTermAxis": "FinanceLeaseMember"}


def test_dimensioned_and_undimensioned_survive_as_distinct_rows():
    """Meta's $45.95B Louisiana fact and its $5.58B consolidated sibling (E6)."""
    facts = ixbrl.parse_instance(META_DUAL)
    hits = ixbrl.select(facts, concept="VariableInterestEntityEntityMaximumLossExposureAmount")
    assert len(hits) == 2
    by_dim = {f.dim_key: f.value for f in hits}
    assert by_dim[""] == 5_580_000_000
    louisiana = [k for k in by_dim if "DataCenterCampusInLouisianaMember" in k]
    assert len(louisiana) == 1
    assert by_dim[louisiana[0]] == 45_950_000_000


def test_instance_values_are_absolute_and_basis_recorded():
    facts = ixbrl.parse_instance(META_DUAL)
    assert all(f.scale_basis == ixbrl.SCALE_BASIS_INSTANCE for f in facts)
    assert all(f.scale is None for f in facts)


def test_dim_key_is_canonical_and_order_independent():
    f = ixbrl.Fact("us-gaap", "X", 1.0, "USD", None, ixbrl.SCALE_BASIS_INSTANCE,
                   None, "2026-06-30", {"BAxis": "M2", "AAxis": "M1"}, "c1")
    assert f.dim_key == "AAxis=M1;BAxis=M2"
    assert f.is_dimensioned is True


def test_undimensioned_fact_has_empty_dim_key():
    f = ixbrl.Fact("us-gaap", "X", 1.0, "USD", None, ixbrl.SCALE_BASIS_INSTANCE,
                   None, "2026-06-30", {}, "c1")
    assert f.dim_key == ""
    assert f.is_dimensioned is False


def test_a_fetched_document_string_parses_as_content_not_a_path():
    """edgar.fetch_document returns TEXT, and the supplier harvest feeds it
    straight to the parser. Treating every str as a path failed the first live
    run with `No such file or directory: '<?xml version="1.0"...'` — after a
    full green suite, because every fixture passed bytes or a real path."""
    doc = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<xbrl xmlns="http://www.xbrl.org/2003/instance" '
        'xmlns:xbrli="http://www.xbrl.org/2003/instance" '
        'xmlns:us-gaap="http://fasb.org/us-gaap/2024">'
        '<xbrli:context id="c1"><xbrli:entity>'
        '<xbrli:identifier scheme="s">0000000001</xbrli:identifier></xbrli:entity>'
        '<xbrli:period><xbrli:startDate>2026-01-01</xbrli:startDate>'
        '<xbrli:endDate>2026-03-31</xbrli:endDate></xbrli:period></xbrli:context>'
        '<xbrli:unit id="u"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>'
        '<us-gaap:Revenues contextRef="c1" unitRef="u" decimals="-6">7</us-gaap:Revenues>'
        '</xbrl>')
    facts = ixbrl.parse_instance(doc)                    # a str, not a path
    assert [f.concept for f in facts] == ["Revenues"]
    assert facts[0].value == 7
    # bytes and a leading-whitespace string must behave identically
    assert len(ixbrl.parse_instance(doc.encode("utf-8"))) == 1
    assert len(ixbrl.parse_instance("\n  " + doc)) == 1


def test_a_real_path_is_still_read_as_a_path(tmp_path):
    p = tmp_path / "inst.xml"
    p.write_text('<?xml version="1.0"?><xbrl xmlns="http://www.xbrl.org/2003/instance"/>',
                 encoding="utf-8")
    assert ixbrl.parse_instance(str(p)) == []
