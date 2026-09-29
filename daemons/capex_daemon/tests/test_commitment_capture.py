"""P2 C2 — the commitments leg through the parser.

companyfacts drops dimensioned facts, so four issuers' commitment figures froze
at whatever they last tagged undimensioned. NVDA published $45.77B while its
filing said $279B of supply and capacity. These tests pin the capture and, more
importantly, the three ways it could silently double-count:

  * a TOTAL tagged as a sibling member of its own components (NVIDIA's $366B),
  * one value wrapped under two contexts (Microsoft's $329.1B, where the
    adversarial re-read found the two nested contexts bound the OPPOSITE way
    round from the first reading), and
  * a maturity ladder on the same member summing to the same total (Amazon's
    six buckets summing to $137,214M).
"""
import pytest

from capex_daemon import commitment_basis as cb
from capex_daemon import commitment_capture as cc
from capex_daemon import dashboard, snapshot


class _F:
    """A parsed fact, shaped like `ixbrl.Fact`."""

    def __init__(self, concept, value, period_end, dims=None, period_start=None):
        self.concept = concept
        self.value = value
        self.period_end = period_end
        self.period_start = period_start
        self.dims = dims or {}


_UUPO = "us-gaap:UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount"
_AXIS = ("us-gaap:UnrecordedUnconditionalPurchaseObligation"
         "ByCategoryOfItemPurchasedAxis")
_NVDA_AXIS = "us-gaap:OtherCommitmentsAxis"


def _rule(ticker, key):
    return next(r for r in cc.rules_for(ticker) if r.key == key)


# --- the map ----------------------------------------------------------------

def test_every_rule_names_the_filing_it_was_read_in():
    """E23: a mapping with no accession is an opinion."""
    for rules in cc.CAPTURE.values():
        for r in rules:
            assert r.accession and r.form and r.filed and r.quote, r.rule_key


def test_no_total_is_ever_a_buildout_row():
    """NVIDIA tags its $366B total as a sibling member of its own components."""
    for rules in cc.CAPTURE.values():
        for r in rules:
            if r.is_total:
                assert not r.buildout, r.rule_key


def test_the_four_frozen_issuers_have_rules():
    for t in ("AMZN", "NVDA", "MU"):
        assert cc.rules_for(t), t
    # CLSK is refused rather than captured, with the reason on file.
    assert not cc.rules_for("CLSK")
    assert "twelve-month" in cc.refusal_for("CLSK")


def test_msft_gains_the_leases_without_losing_its_untagged_status():
    """The $194.06B purchase table is still untagged and still says so; the
    $329.1B of not-yet-commenced leases is a different figure in the same
    filing, and it IS tagged."""
    keys = {r.key for r in cc.rules_for("MSFT")}
    assert keys == {"leases-not-commenced", "construction-commitments"}
    assert cb.class_of("MSFT") == cb.UNCLASSIFIED      # the tagged table: untouched


# --- matching ---------------------------------------------------------------

def test_a_rule_matches_only_its_own_member():
    rule = _rule("AMZN", "leases-not-commenced")
    good = _F(_UUPO, 137_214e6, "2026-06-30",
              {_AXIS: "amzn:OperatingAndFinanceLeasesLeaseNotYetCommencedMember"})
    other = _F(_UUPO, 130_065e6, "2026-06-30",
               {_AXIS: "amzn:LongTermAgreementsToAcquireAndLicenseDigitalMedia"
                       "ContentProcureEnergyAndLicenseSoftwareMember"})
    assert cc.matches(rule, good)
    assert not cc.matches(rule, other)


def test_an_undimensioned_fact_does_not_satisfy_a_dimensioned_rule():
    """The frozen figure is the undimensioned one. Capturing it here would
    reintroduce exactly the number the capture exists to replace."""
    rule = _rule("AMZN", "leases-not-commenced")
    assert not cc.matches(rule, _F(_UUPO, 32_408e6, "2024-06-30"))


def test_a_second_axis_disqualifies_the_fact():
    """A legal-entity or product cut of the same line is a NARROWER figure;
    capturing it as the line would understate."""
    rule = _rule("AMZN", "leases-not-commenced")
    narrower = _F(_UUPO, 40_000e6, "2026-06-30",
                  {_AXIS: "amzn:OperatingAndFinanceLeasesLeaseNotYetCommencedMember",
                   "us-gaap:StatementGeographicalAxis": "country:US"})
    assert not cc.matches(rule, narrower)


def test_a_duration_fact_never_becomes_a_captured_point():
    """A commitment stock is an instant. A duration fact carrying the same
    concept and member is a different measurement and is dropped at capture."""
    rule = _rule("AMZN", "leases-not-commenced")
    flow = _F(_UUPO, 137_214e6, "2026-06-30",
              {_AXIS: "amzn:OperatingAndFinanceLeasesLeaseNotYetCommencedMember"},
              period_start="2026-04-01")
    assert cc.facts_for(rule, [flow]) == {}


# --- the three double-count traps -------------------------------------------

def test_a_value_wrapped_under_two_contexts_is_captured_once():
    """Microsoft's $329.1B: one text node, two lease-term members, identical
    value. Summing would report $658.2B of leases that do not exist."""
    rule = _rule("MSFT", "leases-not-commenced")
    facts = [_F(_UUPO, 329_100e6, "2026-06-30",
                {"us-gaap:LeaseContractualTermAxis": "us-gaap:OperatingLeaseMember"}),
             _F(_UUPO, 329_100e6, "2026-06-30",
                {"us-gaap:LeaseContractualTermAxis": "us-gaap:FinanceLeaseMember"})]
    assert cc.facts_for(rule, facts) == {"2026-06-30": 329_100e6}


def test_the_rule_does_not_depend_on_which_member_wraps_which():
    """The adversarial re-read found the two contexts bound the opposite way
    round from the first reading. A rule keyed on one member name would have
    resolved to the wrong node, so this one accepts either."""
    rule = _rule("MSFT", "leases-not-commenced")
    for member in ("us-gaap:OperatingLeaseMember", "us-gaap:FinanceLeaseMember"):
        got = cc.facts_for(rule, [_F(_UUPO, 329_100e6, "2026-06-30",
                                     {"us-gaap:LeaseContractualTermAxis": member})])
        assert got == {"2026-06-30": 329_100e6}


def test_a_maturity_ladder_on_the_same_member_is_not_summed_into_the_total():
    """Amazon's six buckets sum to the same $137,214M. They are different
    CONCEPTS on the same member, and the rule names one concept."""
    rule = _rule("AMZN", "leases-not-commenced")
    dims = {_AXIS: "amzn:OperatingAndFinanceLeasesLeaseNotYetCommencedMember"}
    facts = [_F(_UUPO, 137_214e6, "2026-06-30", dims)]
    facts += [_F("us-gaap:UnrecordedUnconditionalPurchaseObligationBalanceOn"
                 "FirstAnniversary", 11_732e6, "2026-06-30", dims)]
    assert cc.facts_for(rule, facts) == {"2026-06-30": 137_214e6}


def test_the_total_member_is_captured_but_never_as_buildout():
    """NVIDIA's $366B is real and is published as context; it is the sum of the
    five rows beside it, so it joins nothing."""
    total = _rule("NVDA", "total-first-table")
    assert total.is_total and not total.buildout
    assert total.cls == cb.MIXED_UNSEPARABLE


# --- what the panel does with them ------------------------------------------

def _snap_with_capture(ticker, key, points, bucket="supplier"):
    rule = _rule(ticker, key)
    rows = [{"rule": rule, "points": points}]
    return {"issuers": {ticker: {
        "ticker": ticker, "bucket": bucket, "observations": [],
        "commitments": {"status": "COVERED", "concept": "PurchaseObligation",
                        "points_cq": [], "points": [],
                        "basis": cb.basis_for(ticker).json(),
                        "buildout_row": None,
                        "captured": snapshot._captured_rows(ticker, rows)}}},
        "buckets": {}, "total": {"latest_quarter": "2026Q3", "observations": []},
        "transitions": [], "panel": {}}


NVDA_SUPPLY = {"2026-04-26": 119e9, "2026-07-26": 279e9}


def test_a_captured_move_becomes_a_delta_with_its_own_key_namespace():
    """NVDA's supply and capacity, $119B -> $279B in one quarter — the largest
    forward-demand move on the panel, and invisible to the API."""
    snap = _snap_with_capture("NVDA", "supply-and-capacity", NVDA_SUPPLY)
    d = [x for x in snapshot.commitment_deltas(snap) if x.get("source") == "parser"]
    assert len(d) == 1
    assert d[0]["delta"] == pytest.approx(160e9)
    assert d[0]["basis_class"] == cb.SUPPLY and d[0]["buildout"] is True
    assert d[0]["event_key"].startswith("commitcap:NVDA:supply-and-capacity:")


def test_the_tagged_series_keys_are_unchanged_by_the_capture():
    """A key change re-mints every event and announces years of history as news.
    The parser rows get their own prefix precisely so the old keys survive."""
    snap = {"issuers": {"MARA": {
        "ticker": "MARA", "bucket": "builder", "observations": [],
        "commitments": {"status": "COVERED", "concept": "PurchaseObligation",
                        "points_cq": [{"q": "2026Q1", "value": 1e9},
                                      {"q": "2026Q2", "value": 4e9}]}}},
        "buckets": {}, "total": {"latest_quarter": "2026Q2"}, "transitions": []}
    d = snapshot.commitment_deltas(snap)[0]
    assert d["event_key"] == "commit:MARA:2026Q1:2026Q2:4000000000"
    assert d["source"] == "tagged"


def test_a_captured_total_never_produces_a_delta():
    snap = _snap_with_capture("NVDA", "total-first-table",
                              {"2026-04-26": 200e9, "2026-07-26": 366e9})
    assert [x for x in snapshot.commitment_deltas(snap)
            if x.get("source") == "parser"] == []


def test_a_captured_not_a_commitment_row_never_produces_a_delta():
    snap = _snap_with_capture("NVDA", "equity-investments",
                              {"2026-04-26": 5e9, "2026-07-26": 25e9})
    assert [x for x in snapshot.commitment_deltas(snap)
            if x.get("source") == "parser"] == []


def test_leg_three_plots_the_captured_series_not_the_frozen_one():
    """The whole point: NVDA's $45.77B frozen figure beside Meta's live $349B
    was not a comparison, it was two different years on one axis."""
    snap = _snap_with_capture("NVDA", "supply-and-capacity", NVDA_SUPPLY)
    ser = dashboard.buildout_series(snap)
    assert list(ser) == ["NVDA · supply-and-capacity"]
    assert ser["NVDA · supply-and-capacity"][-1][1] == pytest.approx(279e9)


def test_a_captured_row_publishes_with_its_class_and_its_accession():
    snap = _snap_with_capture("NVDA", "supply-and-capacity", NVDA_SUPPLY)
    row = [r for r in dashboard.commitment_rows(snap) if r["kind"] == "parser"][0]
    assert row["class"] == cb.SUPPLY
    assert row["accession"] == "0001045810-26-000075"
    assert row["as_of"] == "2026-07-26"


def test_the_scan_harvests_and_publishes_the_capture():
    """E36's grep: the caller on the production path, not the definition."""
    import inspect

    from capex_daemon import scan
    src = inspect.getsource(scan)
    assert "_harvest_commitments" in src
    assert "capture_rows=capture_rows" in inspect.getsource(scan.run)


def test_a_frozen_tagged_row_leaves_the_class_ranking_once_captured():
    """NVDA was appearing twice in one class — $45.77B frozen and $8B captured —
    with the superseded figure ranked as though current."""
    snap = _snap_with_capture("NVDA", "capex-obligations",
                              {"2026-07-26": 8e9})
    snap["issuers"]["NVDA"]["commitments"]["points"] = [
        {"end": "2025-07-27", "value": 45.774e9}]
    snap["issuers"]["NVDA"]["commitments"]["latest"] = 45.774e9
    kinds = {r["kind"] for r in dashboard.commitment_rows(snap)}
    assert kinds == {"tagged-superseded", "parser"}
    html = dashboard.view_commitments(snap)
    assert "Superseded" in html


def test_a_refused_capture_is_published_with_its_reason():
    """CLSK: the class split exists only under a twelve-month concept, so there
    is nothing honest to capture. Silence would read as 'nothing here'."""
    snap = _snap_with_capture("NVDA", "capex-obligations", {"2026-07-26": 8e9})
    snap["issuers"]["CLSK"] = {
        "ticker": "CLSK", "bucket": "builder", "observations": [],
        "commitments": {"status": "COVERED", "concept": "ContractualObligation",
                        "points": [{"end": "2024-09-30", "value": 0.268e9}],
                        "points_cq": [], "latest": 0.268e9,
                        "basis": cb.basis_for("CLSK").json(),
                        "captured": [], "capture_refused": cc.refusal_for("CLSK")}}
    html = dashboard.view_commitments(snap)
    assert "CLSK: capture refused" in html and "twelve-month" in html
