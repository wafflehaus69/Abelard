"""CD-GAP2A A5 — a ratio that agrees with itself to three figures.

IREN published credit/capex of exactly +100%. Measured, the two legs are
$2,998,006,000 and $3,000,000,000: equal to three significant figures, not the
same number, and drawn from `PaymentsToAcquirePropertyPlantAndEquipment` and
`ProceedsFromConvertibleDebt`, which share no fact. Convertible notes are issued
in round amounts and a real capex figure landed 0.07% away from one.

So the answer for IREN is coincidence — but the shape is exactly what one fact
resolved into both legs would look like, and that would make the ratio a
tautology at the precise point the thesis is tested. Hence a gate, and hence the
gate distinguishing the two cases rather than crying wolf.
"""
from capex_daemon import divergence as dv


def test_three_significant_figures_is_the_trigger():
    assert dv.suspect_identity(2_998_006_000, 3_000_000_000)      # IREN, live
    assert dv.suspect_identity(1_000_000, 1_004_000)              # 1.00e6 both
    assert not dv.suspect_identity(1_000_000, 1_010_000)          # 1.00 vs 1.01
    assert not dv.suspect_identity(3.0e9, 6.0e9)


def test_a_missing_or_zero_leg_is_never_suspicious():
    """An explicitly tagged zero on both legs is a real double zero, not an
    agreement — MSFT tags 0 debt proceeds and that is a fact (E16)."""
    assert not dv.suspect_identity(None, 3.0e9)
    assert not dv.suspect_identity(3.0e9, None)
    assert not dv.suspect_identity(0.0, 0.0)
    assert not dv.suspect_identity(3.0e9, 0.0)


def test_significant_figures_not_absolute_difference():
    """$2.998B vs $3.000B is a $2M gap and suspicious; $2M vs $4M is a $2M gap
    and not. The rule has to be relative or it is meaningless across four orders
    of magnitude of issuer size."""
    assert dv.suspect_identity(2_998_006_000, 3_000_000_000)
    assert not dv.suspect_identity(2_000_000, 4_000_000)


def test_a_shared_concept_is_a_defect_not_a_coincidence():
    assert dv.shares_a_concept("PaymentsToAcquirePropertyPlantAndEquipment",
                               ["PaymentsToAcquirePropertyPlantAndEquipment"])
    assert not dv.shares_a_concept("PaymentsToAcquirePropertyPlantAndEquipment",
                                   ["ProceedsFromConvertibleDebt"])   # IREN, live
    assert not dv.shares_a_concept(None, ["X"])
    assert not dv.shares_a_concept("X", [])


def test_the_two_statuses_are_distinct_and_only_one_refuses():
    """SUSPECT-IDENTITY publishes the ratio with a flag; RATIO-TAUTOLOGY refuses
    it, because one fact divided by itself is not a measurement."""
    assert dv.STATUS_SUSPECT_IDENTITY != dv.STATUS_RATIO_TAUTOLOGY
    assert dv.STATUS_SUSPECT_IDENTITY == "SUSPECT-IDENTITY"
    assert dv.STATUS_RATIO_TAUTOLOGY == "RATIO-TAUTOLOGY"


# --- B7: a flag must state its own resolution ------------------------------

def test_a_resolved_suspicion_says_so_in_the_status():
    """A flag with no resolution is a question left open on the page forever.
    IREN was investigated and cleared; the status has to carry that."""
    res = dv.identity_resolution("IREN")
    assert res["verdict"] == "COINCIDENCE" and res["checked"] == "2026-09-02"
    assert "ProceedsFromConvertibleDebt" in res["evidence"]
    assert dv.STATUS_SUSPECT_VERIFIED_COINCIDENCE.endswith("VERIFIED-COINCIDENCE")


def test_an_unresolved_suspicion_keeps_the_bare_flag():
    assert dv.identity_resolution("SOMEONE-ELSE") is None


def test_the_resolution_is_visible_on_the_issuer_row():
    from capex_daemon import universe
    e = universe.Entity("0001878848", "IREN", "builder", "", "")
    ix = _iren_index()
    v = dv.build_issuer_view(e, ix)
    assert dv.STATUS_SUSPECT_VERIFIED_COINCIDENCE in v.statuses
    assert dv.STATUS_SUSPECT_IDENTITY not in v.statuses


def _iren_index():
    """A miniature IREN: disjoint concepts landing 0.07% apart."""
    from capex_daemon.facts_api import ApiFact
    def f(concept, start, end, val):
        return ApiFact(concept=concept, taxonomy="us-gaap", unit="USD", value=val,
                       period_start=start, period_end=end, duration_days=91,
                       form="10-Q", filed=end, frame=None)
    qs = [("2025-07-01", "2025-09-30"), ("2025-10-01", "2025-12-31"),
          ("2026-01-01", "2026-03-31"), ("2026-04-01", "2026-06-30")]
    return {
        "PaymentsToAcquirePropertyPlantAndEquipment":
            [f("PaymentsToAcquirePropertyPlantAndEquipment", a, b, 749_501_500.0)
             for a, b in qs],
        "ProceedsFromConvertibleDebt":
            [f("ProceedsFromConvertibleDebt", a, b, 750_000_000.0) for a, b in qs],
    }
