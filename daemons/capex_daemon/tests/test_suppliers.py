"""CD-3 acceptance tests: the supplier leg and the cross-check.

Every fixture below mirrors a shape measured in a real filing — the axis
variation, the case variation and the rotated members are all things NVDA and
AMD actually did, not hypotheticals.
"""
import sqlite3


class _F:
    """A minimal parsed-fact stand-in for the mapped-unit tests."""

    def __init__(self, concept, value, period_start, period_end, dims):
        self.concept, self.value = concept, value
        self.period_start, self.period_end = period_start, period_end
        self.dims, self.scale_basis = dims, "ixbrl"

    @property
    def dim_key(self):
        return ";".join("{}={}".format(a, m) for a, m in sorted(self.dims.items()))


import pytest

from capex_daemon import dashboard, snapshot, storage, suppliers, trend, universe


class Fact:
    def __init__(self, concept, value, start, end, dims=None):
        self.concept = concept
        self.value = value
        self.period_start = start
        self.period_end = end
        self.dims = dims or {}
        self.scale_basis = "ixbrl"

    @property
    def dim_key(self):
        return ";".join("{}={}".format(a, m) for a, m in sorted(self.dims.items()))


class Ent:
    def __init__(self, cik="1045810", ticker="NVDA", bucket="supplier"):
        self.cik = cik
        self.ticker_display = ticker
        self.bucket = bucket


REV = "Revenues"
RFC = "RevenueFromContractWithCustomerExcludingAssessedTax"
PRODUCT = "us-gaap:ProductOrServiceAxis"
SEGMENT = "us-gaap:StatementBusinessSegmentsAxis"
GEO = "us-gaap:StatementGeographicalAxis"
CONSOL = "us-gaap:ConsolidationItemsAxis"
DC = "nvda:DataCenterMember"


# --- the doctrine: never blended -------------------------------------------

def test_suppliers_are_never_summed_into_the_spending_aggregate():
    """A supplier's revenue and a builder's capex are the same dollar seen from
    opposite ends. Adding them double-counts it (CD-R2 2.3)."""
    assert suppliers.SUPPLIER_BUCKET not in trend.AGGREGATED_BUCKETS
    assert suppliers.SUPPLIER_BUCKET in universe.BUCKETS


def test_the_live_roster_keeps_every_supplier_out_of_the_aggregates():
    roster = universe.load()
    sup = [e for e in roster.values() if e.bucket == suppliers.SUPPLIER_BUCKET]
    assert {e.ticker_display for e in sup} == {"NVDA", "AMD", "AVGO", "MU", "SMCI"}
    for e in sup:
        assert e.bucket not in trend.AGGREGATED_BUCKETS


def test_a_non_supplier_is_refused_rather_than_parsed():
    leg = suppliers.build_leg(Ent(bucket="hyperscaler"), [("2026-01-01", [])])
    assert leg.status == suppliers.STATUS_NOT_A_SUPPLIER


# --- resolution: three things vary, all measured ---------------------------

def test_the_member_is_matched_on_any_axis():
    """NVDA files on ProductOrServiceAxis, AMD on StatementBusinessSegmentsAxis."""
    for axis in (PRODUCT, SEGMENT):
        pairs, axes, _r = suppliers.dc_facts(
            [("2026-01-01", [Fact(REV, 100.0, "2025-10-01", "2025-12-31", {axis: DC})])])
        assert len(pairs) == 1, axis
        assert axes == [suppliers._local(axis)]


def test_the_member_is_matched_case_insensitively():
    """AMD's 10-Q says DataCenterMember and its 10-K says DatacenterMember.
    Exact matching lost every Q4 — which is derivable only from the 10-K — and
    left AMD with no TTM at all."""
    pairs, _a, _r = suppliers.dc_facts([("2026-01-01", [
        Fact(RFC, 1.0, "2025-01-01", "2025-12-31", {SEGMENT: "amd:DatacenterMember"})])])
    assert len(pairs) == 1


def test_both_revenue_concepts_resolve():
    """NVDA uses `Revenues`; AMD uses `RevenueFromContractWithCustomer...`."""
    for concept in (REV, RFC):
        pairs, _a, _r = suppliers.dc_facts(
            [("2026-01-01", [Fact(concept, 5.0, "2025-10-01", "2025-12-31",
                                  {PRODUCT: DC})])])
        assert len(pairs) == 1, concept


def test_a_geographic_breakdown_of_the_line_is_not_the_line():
    """`DataCenter x Americas` is a slice. Taking it undercounts."""
    pairs, _a, _r = suppliers.dc_facts([("2026-01-01", [
        Fact(REV, 75.0, "2025-10-01", "2025-12-31", {PRODUCT: DC}),
        Fact(REV, 20.0, "2025-10-01", "2025-12-31",
             {PRODUCT: DC, GEO: "country:US"})])])
    assert len(pairs) == 1
    assert pairs[0][0].value == 75.0


def test_a_segment_qualifier_is_not_a_breakdown():
    """AMD tags its datacenter line with ConsolidationItemsAxis=OperatingSegments.
    Treating that as a breakdown refused AMD outright on the first run."""
    pairs, _a, _r = suppliers.dc_facts([("2026-01-01", [
        Fact(RFC, 6.7, "2026-03-29", "2026-06-27",
             {SEGMENT: DC, CONSOL: "us-gaap:OperatingSegmentsMember"})])])
    assert len(pairs) == 1 and pairs[0][0].value == 6.7


# --- restatement: newest filing wins ---------------------------------------

def test_the_newest_filing_supersedes_a_mis_tagged_one():
    """MEASURED: AMD's 10-Q for the quarter ending 2024-03-30 tagged its segment
    members ROTATED — $2,337M (Data Center) carried ClientMember, and
    DataCenterMember carried $922M, which is Gaming. The filing a year later
    restates it correctly. Anything but recency publishes a 2.5x undercount."""
    stale = Fact(RFC, 922e6, "2023-12-31", "2024-03-30", {SEGMENT: DC})
    fixed = Fact(RFC, 2337e6, "2023-12-31", "2024-03-30", {SEGMENT: DC})
    pairs, _a, restated = suppliers.dc_facts(
        [("2024-03-30", [stale]), ("2025-03-29", [fixed])])
    assert len(pairs) == 1 and pairs[0][0].value == 2337e6
    assert len(restated) == 1
    assert restated[0]["was"] == 922e6 and restated[0]["now"] == 2337e6
    assert restated[0]["superseded_by"] == "2025-03-29"


def test_filing_order_does_not_depend_on_input_order():
    stale = Fact(RFC, 922e6, "2023-12-31", "2024-03-30", {SEGMENT: DC})
    fixed = Fact(RFC, 2337e6, "2023-12-31", "2024-03-30", {SEGMENT: DC})
    for batches in ([("2025-03-29", [fixed]), ("2024-03-30", [stale])],
                    [("2024-03-30", [stale]), ("2025-03-29", [fixed])]):
        leg = suppliers.build_leg(Ent(), sorted(batches))
        assert leg.quarters["2024Q1"] == 2337e6


def test_a_restatement_is_counted_in_the_published_detail():
    leg = suppliers.build_leg(Ent(), [
        ("2024-03-30", [Fact(RFC, 1.0, "2023-12-31", "2024-03-30", {SEGMENT: DC})]),
        ("2025-03-29", [Fact(RFC, 2.0, "2023-12-31", "2024-03-30", {SEGMENT: DC})])])
    assert leg.restatements and "superseded" in leg.detail


# --- refusals name what they saw -------------------------------------------

def test_no_datacenter_member_refuses_and_cites_the_members_it_found():
    """MU's business-unit codes bear on the buildout, but mapping CMBU+CDBU to
    'datacenter' is a ruling, not a parse."""
    leg = suppliers.build_leg(Ent(ticker="MU"), [("2026-05-28", [
        Fact(RFC, 11.5, "2026-03-01", "2026-05-28", {SEGMENT: "mu:CDBUMember"}),
        Fact(RFC, 13.7, "2026-03-01", "2026-05-28", {SEGMENT: "mu:CMBUMember"})])])
    assert leg.status == suppliers.STATUS_NO_DC_MEMBER
    assert "CDBUMember" in leg.detail and "CMBUMember" in leg.detail
    assert not leg.is_covered


def test_no_instances_is_distinct_from_no_member():
    assert suppliers.build_leg(Ent(), []).status == suppliers.STATUS_NO_INSTANCES


# --- the cross-check is a ratio, never a sum -------------------------------

def test_crosscheck_relates_by_ratio_and_never_adds():
    dc = {"2026Q1": 200.0, "2026Q2": 250.0}
    capex = {"2026Q1": 400.0, "2026Q2": 500.0}
    out = suppliers.crosscheck(dc, capex)
    assert out["ratio"] == pytest.approx(0.5)
    for row in out["quarters"]:
        assert row["ratio"] == row["dc"] / row["capex"]
        assert "total" not in row and "sum" not in row


def test_crosscheck_without_overlap_refuses_rather_than_guessing():
    out = suppliers.crosscheck({"2020Q1": 1.0}, {"2026Q1": 1.0})
    assert out["status"] == suppliers.STATUS_NO_OVERLAP and out["ratio"] is None


def test_a_ratio_whose_denominator_lost_a_member_is_flagged():
    """MEASURED live: 2026Q2 reads 52.8% against 44.1% the quarter before, and
    Meta simply has not filed. The move is arithmetic, not economic."""
    class BT:
        ttm = {"2026Q1": 1000.0, "2026Q2": 900.0}
        membership = {"2026Q1": ["A", "B", "C", "D", "E"], "2026Q2": ["A", "B", "C", "D"]}
    legs = {"NVDA": suppliers.SupplierLeg(
        "1", "NVDA", suppliers.STATUS_COVERED,
        quarters={"{}Q{}".format(y, q): 100.0
                  for y in (2025, 2026) for q in (1, 2, 3, 4)})}
    sec = snapshot._supplier_section(legs, {"hyperscaler": BT()})
    assert sec["crosscheck"]["warning"] and "membership change" in sec["crosscheck"]["warning"]


def test_no_warning_when_membership_held():
    class BT:
        ttm = {"2026Q1": 1000.0, "2026Q2": 900.0}
        membership = {"2026Q1": ["A", "B"], "2026Q2": ["A", "B"]}
    legs = {"NVDA": suppliers.SupplierLeg(
        "1", "NVDA", suppliers.STATUS_COVERED,
        quarters={"{}Q{}".format(y, q): 100.0
                  for y in (2025, 2026) for q in (1, 2, 3, 4)})}
    sec = snapshot._supplier_section(legs, {"hyperscaler": BT()})
    assert sec["crosscheck"]["warning"] is None


# --- caching: the nightly must not re-fetch fourteen filings ---------------

@pytest.fixture()
def con():
    c = sqlite3.connect(":memory:")
    c.executescript(storage.SCHEMA)
    return c


SUBS = {"filings": {"recent": {
    "form": ["10-Q", "10-K"],
    "reportDate": ["2026-04-26", "2026-01-25"],
    "accessionNumber": ["0001045810-26-000052", "0001045810-26-000010"],
    "primaryDocument": ["nvda-20260426.htm", "nvda-20260125.htm"]}}}

INSTANCE = (
    b"<?xml version='1.0'?><xbrli:xbrl xmlns:xbrli='http://www.xbrl.org/2003/instance' "
    b"xmlns:xbrldi='http://xbrl.org/2006/xbrldi' xmlns:us-gaap='http://x' "
    b"xmlns:nvda='http://y'>"
    b"<xbrli:context id='c1'><xbrli:entity><xbrli:identifier scheme='s'>1</xbrli:identifier>"
    b"<xbrli:segment><xbrldi:explicitMember dimension='us-gaap:ProductOrServiceAxis'>"
    b"nvda:DataCenterMember</xbrldi:explicitMember></xbrli:segment></xbrli:entity>"
    b"<xbrli:period><xbrli:startDate>2026-01-26</xbrli:startDate>"
    b"<xbrli:endDate>2026-04-26</xbrli:endDate></xbrli:period></xbrli:context>"
    b"<xbrli:unit id='usd'><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>"
    b"<us-gaap:Revenues contextRef='c1' unitRef='usd' decimals='-6'>75246000000"
    b"</us-gaap:Revenues></xbrli:xbrl>")


def test_harvest_is_idempotent_and_caches_by_instance(con):
    calls = []

    def fetch(cik, accession, docname, http=None):
        calls.append(docname)
        return INSTANCE

    e = Ent()
    added, failed = suppliers.harvest(e, con, submissions_doc=SUBS, fetch=fetch)
    assert added == 2 and not failed and len(calls) == 2
    added2, _ = suppliers.harvest(e, con, submissions_doc=SUBS, fetch=fetch)
    assert added2 == 0 and len(calls) == 2       # nothing re-fetched


def test_an_instance_with_no_member_is_not_refetched_forever(con):
    """Otherwise AVGO, MU and SMCI cost 14 document fetches every single night."""
    empty = INSTANCE.replace(b"nvda:DataCenterMember", b"nvda:GamingMember")
    calls = []

    def fetch(cik, accession, docname, http=None):
        calls.append(docname)
        return empty

    e = Ent(ticker="AVGO")
    suppliers.harvest(e, con, submissions_doc=SUBS, fetch=fetch)
    suppliers.harvest(e, con, submissions_doc=SUBS, fetch=fetch)
    assert len(calls) == 2
    assert suppliers.leg_from_db(e, con).status == suppliers.STATUS_NO_DC_MEMBER


def test_a_leg_rebuilt_from_the_db_matches_the_leg_built_from_facts(con):
    e = Ent()
    suppliers.harvest(e, con, submissions_doc=SUBS,
                      fetch=lambda *a, **k: INSTANCE)
    leg = suppliers.leg_from_db(e, con)
    assert leg.status == suppliers.STATUS_COVERED
    assert leg.quarters["2026Q2"] == 75246000000.0
    assert leg.axes == ["ProductOrServiceAxis"]


def test_a_broken_instance_costs_one_filing_not_the_leg(con):
    def fetch(cik, accession, docname, http=None):
        if "20260125" in docname:
            raise RuntimeError("truncated")
        return INSTANCE

    e = Ent()
    added, failed = suppliers.harvest(e, con, submissions_doc=SUBS, fetch=fetch)
    assert added == 1 and len(failed) == 1
    assert suppliers.leg_from_db(e, con).status == suppliers.STATUS_COVERED


# --- the published view ----------------------------------------------------

def test_the_supplier_view_renders_and_is_in_the_nav():
    assert ("/suppliers", "Suppliers") in dashboard.VIEWS
    assert dashboard.ROUTES["/suppliers"] is dashboard.view_suppliers


def test_an_uncovered_supplier_publishes_a_status_not_a_zero():
    legs = {"MU": suppliers.SupplierLeg("1", "MU", suppliers.STATUS_NO_DC_MEMBER,
                                        "business-unit codes only")}
    sec = snapshot._supplier_section(legs, {})
    assert sec["legs"]["MU"]["ttm"] is None
    assert sec["legs"]["MU"]["status"] == suppliers.STATUS_NO_DC_MEMBER
    assert sec["covered"] == []


# --- ruling 2026-08-21: MU admitted via a RULED unit mapping ----------------

def test_a_mapped_leg_never_reports_itself_as_measured():
    """The condition on the ruling: a semantic judgement stays a disclosed one."""
    assert suppliers.STATUS_MAPPED == "MAPPED-BUSINESS-UNITS"
    assert suppliers.STATUS_MAPPED != suppliers.STATUS_COVERED
    leg = suppliers.SupplierLeg("x", "MU", suppliers.STATUS_MAPPED,
                                quarters={"2026Q1": 1.0})
    assert leg.is_covered and leg.is_mapped     # usable, and never mistakable


def test_the_mapping_carries_its_ruling_date_and_both_unit_lists():
    spec = suppliers.MAPPED_UNITS["0000723125"]
    assert spec["ruled"] == "2026-08-21"
    assert set(spec["members"]) == {"CMBUMember", "CDBUMember"}
    # Explicitly NOT datacenter demand, and named so a reader can disagree.
    assert "MCBUMember" in spec["excluded"] and "AEBUMember" in spec["excluded"]
    assert not set(spec["members"]) & set(spec["excluded"])


def test_mapped_units_are_summed_and_unruled_units_are_ignored():
    def fact(member, value, ps, pe):
        return _F("Revenues", value, ps, pe,
                  {"StatementBusinessSegmentsAxis": member})
    spec = suppliers.MAPPED_UNITS["0000723125"]
    batch = [("2026-05-28", [
        fact("CMBUMember", 13.77e9, "2026-02-27", "2026-05-28"),
        fact("CDBUMember", 11.52e9, "2026-02-27", "2026-05-28"),
        fact("MCBUMember", 99.0e9, "2026-02-27", "2026-05-28"),      # excluded
        fact("AEBUMember", 88.0e9, "2026-02-27", "2026-05-28"),      # excluded
    ])]
    out, axes, partial = suppliers.mapped_facts(batch, spec)
    assert len(out) == 1 and not partial
    assert out[0][0].value == pytest.approx(25.29e9)   # CMBU + CDBU only
    assert axes == ["StatementBusinessSegmentsAxis"]


def test_a_period_missing_one_mapped_unit_is_skipped_not_half_summed():
    """A partial sum understates the line and reads as a decline."""
    spec = suppliers.MAPPED_UNITS["0000723125"]
    batch = [("2026-05-28", [
        _F("Revenues", 13.77e9, "2026-02-27", "2026-05-28",
           {"StatementBusinessSegmentsAxis": "CMBUMember"}),
    ])]
    out, _axes, partial = suppliers.mapped_facts(batch, spec)
    assert out == []
    assert partial and partial[0]["missing"] == ["CDBUMember"]


def test_an_issuer_with_no_ruling_gets_no_mapping():
    class E:
        cik, ticker_display, bucket = "0001730168", "AVGO", "supplier"
    assert suppliers.mapping_for(E()) is None


def test_the_dashboard_prints_the_mapping_beside_the_number():
    """'a semantic judgment stays a disclosed semantic judgment' — the ruling."""
    from capex_daemon import dashboard
    spec = suppliers.MAPPED_UNITS["0000723125"]
    snap = {"suppliers": {"legs": {"MU": {
        "ticker": "MU", "status": suppliers.STATUS_MAPPED, "detail": "d",
        "ttm": 52.5e9, "quarters": [], "axes": [], "restatements": [],
        "restatement_count": 0, "mapping": spec, "is_mapped": True}},
        "crosscheck": {}}}
    html = dashboard.view_suppliers(snap)
    assert "MAPPED-BUSINESS-UNITS" in html
    assert "2026-08-21" in html and "Mando" in html
    assert "Cloud Memory Business Unit" in html          # what was summed
    assert "Mobile and Client" in html                   # what was excluded
    assert "mapped, not measured" in html
