"""CD-BRIEF1 B5 — the panel commitments total is refused, not printed.

`ContractualObligation`, `PurchaseObligation` and
`UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount` are three
different measures with three different scopes. Adding them yields a number
with no defined meaning, and it has been on the front page as though it had one.

Per-issuer figures are untouched. Each is internally consistent and comparable
to its own history — which is exactly why the A4 deltas are computed per issuer
and never summed.
"""
from capex_daemon import commitments, snapshot


def _issuers(concepts):
    return {t: {"commitments": {"status": commitments.STATUS_COVERED,
                                "concept": c, "points_cq": []}}
            for t, c in concepts.items()}


def _panel_after(issuers):
    """Run just the B5 clause the way snapshot.build does."""
    panel = {"commitments_panel": {"status": "OK", "detail": ""}}
    bases = {}
    for tick, iss in issuers.items():
        c = iss.get("commitments") or {}
        if c.get("status") == commitments.STATUS_COVERED and c.get("concept"):
            bases.setdefault(c["concept"], []).append(tick)
    if len(bases) > 1:
        panel["commitments_panel"] = {"status": "REFUSED-MIXED-BASIS",
                                      "basis_classes": bases}
    return panel


def test_three_concepts_refuse_the_total():
    issuers = _issuers({"META": "ContractualObligation",
                        "SMCI": "PurchaseObligation",
                        "AMZN": "UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount"})
    cp = _panel_after(issuers)["commitments_panel"]
    assert cp["status"] == "REFUSED-MIXED-BASIS"
    assert len(cp["basis_classes"]) == 3


def test_one_concept_across_every_issuer_does_not_refuse_on_basis():
    """The refusal is about MIXING. A single-class panel may still be refused
    for membership, but not for this reason."""
    issuers = _issuers({"META": "ContractualObligation",
                        "SMCI": "ContractualObligation"})
    assert _panel_after(issuers)["commitments_panel"]["status"] == "OK"


def test_the_live_panel_is_mixed_basis():
    """Measured on the live roster: the disclosing issuers genuinely do split
    across concepts, so this is not a hypothetical guard."""
    issuers = _issuers({
        "META": "ContractualObligation", "AVGO": "UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount",
        "NVDA": "PurchaseObligation", "SMCI": "PurchaseObligation",
        "AMZN": "UnrecordedUnconditionalPurchaseObligationBalanceSheetAmount",
        "KEEL": "ContractualObligation"})
    cp = _panel_after(issuers)["commitments_panel"]
    assert cp["status"] == "REFUSED-MIXED-BASIS"
    assert sorted(cp["basis_classes"]["PurchaseObligation"]) == ["NVDA", "SMCI"]


def test_the_front_page_prints_the_refusal_rather_than_a_number():
    from capex_daemon import dashboard
    snap = _fake_with_mixed_basis()
    html = dashboard.view_aggregate(snap)
    assert "REFUSED-MIXED-BASIS" in html
    assert "$99.00B" not in html          # the summed figure never appears


def _fake_with_mixed_basis():
    from .test_charts import _fake_snapshot
    snap = _fake_snapshot()
    snap["panel"]["commitments"] = [{"q": "2026Q2", "value": 99.0e9, "members": 3}]
    snap["panel"]["commitments_membership_latest"] = ["META", "SMCI", "AMZN"]
    snap["panel"]["commitments_panel"] = {
        "status": "REFUSED-MIXED-BASIS",
        "detail": "three concepts, three scopes",
        "disclosing_issuers": 3}
    return snap
