"""ORDER CD-GAP2-P2-CLOSE Z1 — the three rulings of 2026-09-28, pinned.

* NVDA's $36B of AI-cloud agreements stay SUPPLY with a CONTINGENT sub-tag.
* GUARANTEES is a class of its own: never buildout, never "not a commitment".
* AMZN's $130.065B line is reclassified MIXED-UNSEPARABLE, DATED from
  2025-03-31 — the 10-Q in which "acquire property and equipment" entered its
  footnote. Earlier points keep CONTENT-ENERGY-SOFTWARE, which was exact then.
"""
from capex_daemon import alerts, dashboard, snapshot
from capex_daemon import commitment_basis as cb
from capex_daemon import commitment_capture as cc


def _rule(ticker, key):
    return next(r for r in cc.rules_for(ticker) if r.key == key)


def _snap(ticker, key, points):
    rows = [{"rule": _rule(ticker, key), "points": points}]
    return {"issuers": {ticker: {
        "ticker": ticker, "bucket": "supplier", "observations": [],
        "commitments": {"status": "COVERED", "concept": "x", "points_cq": [],
                        "points": [], "basis": cb.basis_for(ticker).json(),
                        "buildout_row": None,
                        "captured": snapshot._captured_rows(ticker, rows)}}},
        "buckets": {}, "total": {"latest_quarter": "2026Q3"}, "transitions": []}


# --- CONTINGENT -------------------------------------------------------------

def test_the_ai_cloud_agreements_stay_supply_and_carry_the_sub_tag():
    r = _rule("NVDA", "ai-cloud-agreements")
    assert r.cls == cb.SUPPLY and r.buildout
    assert r.qualifiers == (cb.QUALIFIER_CONTINGENT,)


def test_the_sub_tag_travels_into_the_delta_and_the_alert_payload(tmp_path):
    from abelard_common.alert_queue import AlertQueue
    # 2.8x and +$36B: past the absolute arm, under the 10x BASIS-SUSPECT line.
    snap = _snap("NVDA", "ai-cloud-agreements",
                 {"2026-04-26": 20e9, "2026-07-26": 56e9})
    d = [x for x in snapshot.commitment_deltas(snap) if x.get("source") == "parser"]
    assert d and d[0]["qualifiers"] == [cb.QUALIFIER_CONTINGENT]
    q = AlertQueue(tmp_path / "q.db")
    try:
        rows, _ = snapshot.commitment_alerts_and_quarantine(snap)
        alerts.enqueue_commitment_alerts(rows, queue=q)
        item = [i for i in q.items() if i.kind == alerts.KIND_COMMITMENT_MOVE][0]
        assert item.payload["qualifiers"] == [cb.QUALIFIER_CONTINGENT]
    finally:
        q.close()


def test_the_sub_tag_is_visible_on_the_page():
    snap = _snap("NVDA", "ai-cloud-agreements", {"2026-07-26": 36e9})
    assert "CONTINGENT" in dashboard.view_commitments(snap)


# --- GUARANTEES -------------------------------------------------------------

def test_guarantees_is_a_class_and_is_never_buildout():
    assert cb.GUARANTEES in cb.CLASS_ORDER
    assert cb.GUARANTEES not in cb.BUILDOUT_CLASSES
    assert cb.GUARANTEES not in cb.EXCLUDED_CLASSES


# --- AMZN, dated ------------------------------------------------------------

def test_the_reclass_is_dated_to_the_filing_that_changed_the_wording():
    r = _rule("AMZN", "content-energy-software")
    assert r.cls == cb.MIXED_UNSEPARABLE
    assert r.cls_before == cb.CONTENT_ENERGY_SOFTWARE
    assert r.reclass_from == "2025-03-31"
    assert "0001018724-25-000036" in r.reclass_evidence


def test_points_before_the_date_keep_the_class_they_had():
    r = _rule("AMZN", "content-energy-software")
    assert r.cls_at("2024-12-31") == cb.CONTENT_ENERGY_SOFTWARE
    assert r.cls_at("2025-03-31") == cb.MIXED_UNSEPARABLE
    assert r.cls_at("2026-06-30") == cb.MIXED_UNSEPARABLE


def test_a_move_across_the_reclass_is_a_basis_change_not_a_move():
    """Q4 2024 -> Q1 2025 spans the re-wording: the line started including
    property and equipment. Whatever it did in size, it is not comparable."""
    snap = _snap("AMZN", "content-energy-software",
                 {"2024-12-31": 70e9, "2025-03-31": 80e9})
    d = [x for x in snapshot.commitment_deltas(snap) if x.get("source") == "parser"][0]
    assert d["basis_change"] is True and d["buildout"] is False


def test_the_frozen_tagged_figure_keeps_its_exact_class():
    """AMZN's frozen 2024Q2 $32.41B had no PP&E in its footnote."""
    assert cb.class_of("AMZN") == cb.CONTENT_ENERGY_SOFTWARE


# --- Z3: NVIDIA's guarantees, read and captured ------------------------------

class _F:
    def __init__(self, concept, value, period_end, dims=None):
        self.concept, self.value, self.period_end = concept, value, period_end
        self.period_start, self.dims = None, dims or {}


_G = "us-gaap:GuaranteeObligationsMaximumExposure"
_GAX = "us-gaap:GuaranteeObligationsByNatureAxis"


def test_both_guarantees_are_guarantees_and_neither_is_buildout():
    for key in ("guarantee-ai-cloud-leases", "guarantee-openai-ports"):
        r = _rule("NVDA", key)
        assert r.cls == cb.GUARANTEES and not r.buildout


def test_the_subsequent_event_copy_of_the_105b_is_not_captured_twice():
    """The filing tags $105B at 2026-07-26 AND, as a subsequent event with a
    counterparty axis, at 2026-08-31. A naive sum of every fact on the concept
    reads $325.5B."""
    facts = [_F(_G, 105e9, "2026-07-26", {_GAX: "us-gaap:FinancialGuaranteeMember"}),
             _F(_G, 105e9, "2026-08-31", {_GAX: "us-gaap:FinancialGuaranteeMember",
                                          "srt:CounterpartyNameAxis": "nvda:SBEnergyCorp.Member",
                                          "us-gaap:SubsequentEventTypeAxis":
                                              "us-gaap:SubsequentEventMember"})]
    assert cc.facts_for(_rule("NVDA", "guarantee-openai-ports"), facts) == \
        {"2026-07-26": 105e9}


def test_the_renamed_member_keeps_the_series_continuous():
    """FacilityLeaseGuaranteesMember until Q1, LandPowerAndShell... from Q2."""
    r = _rule("NVDA", "guarantee-ai-cloud-leases")
    facts = [_F(_G, 3.5e9, "2026-01-25", {_GAX: "nvda:FacilityLeaseGuaranteesMember"}),
             _F(_G, 3.5e9, "2026-07-26",
                {_GAX: "nvda:LandPowerAndShellGuaranteesForAICloudsMember"})]
    assert cc.facts_for(r, facts) == {"2026-01-25": 3.5e9, "2026-07-26": 3.5e9}


def test_the_total_does_not_vouch_for_history_nobody_read():
    r = _rule("NVDA", "guarantees-total")
    facts = [_F(_G, 0.86e9, "2025-10-26"), _F(_G, 108.5e9, "2026-07-26")]
    assert cc.facts_for(r, facts) == {"2026-07-26": 108.5e9}
    assert r.is_total
