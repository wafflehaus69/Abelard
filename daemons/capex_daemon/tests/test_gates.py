"""B7 acceptance tests for CD-G3 TTM anchor reconciliation (R2a)."""
from capex_daemon import config, gates, tagmap


class FakeRes:
    def __init__(self, concept=None, multi=(), unresolved=False):
        self._c = concept
        self.multi_line_concepts = tuple(multi)
        self._unresolved = unresolved
        self.eras = () if unresolved else (1,)

    @property
    def is_multi_line(self):
        return bool(self.multi_line_concepts)

    @property
    def is_unresolved(self):
        return self._unresolved

    @property
    def current_concept(self):
        return self._c

    def concept_for(self, _):
        return self._c


def test_band_is_an_order_of_magnitude_bound_not_a_precision_one():
    lo, hi = config.ANCHOR_BAND
    for measured in (0.98, 1.01, 0.91, 0.73, 1.06):   # observed across the panel
        assert lo <= measured <= hi
    assert not (lo <= 23.3 <= hi)                     # the AMZN stale-resolution class


def test_missing_anchor_concept_is_unanchored_not_approximated():
    c = gates.reconcile("x", {}, FakeRes("Capex"), FakeRes(unresolved=True))
    assert c.verdict == gates.VERDICT_UNANCHORED
    assert c.ratio is None
    assert "no gross-basis anchor" in c.detail


def test_disagreeing_anchor_concepts_are_unanchored():
    """EQIX carries PropertyPlantAndEquipmentGross and RealEstateGrossAtCarryingValue
    disagreeing near the frontier; neither may be silently preferred."""
    c = gates.reconcile("x", {}, FakeRes("Capex"),
                        FakeRes(multi=("PropertyPlantAndEquipmentGross",
                                       "RealEstateGrossAtCarryingValue")))
    assert c.verdict == gates.VERDICT_UNANCHORED
    assert "disagree" in c.detail


def test_nearest_instant_respects_the_tolerance():
    inst = {"2026-03-31": 100.0, "2025-12-31": 80.0}
    assert gates._nearest_instant(inst, "2026-03-31") == ("2026-03-31", 100.0)
    # A quarter away is not "nearest" — it is absent.
    assert gates._nearest_instant(inst, "2026-06-30") is None


def test_a_stale_anchor_series_reports_unanchored_rather_than_reaching():
    """CORZ has the concept but its instants stop at 2024-06-30; reaching back a
    year and a half to close a 2026 window would fabricate a reconciliation."""
    assert gates._nearest_instant({"2024-06-30": 1.0}, "2026-06-30") is None


def test_verdicts_are_distinct_states():
    assert len({gates.VERDICT_RECONCILED, gates.VERDICT_FLAGGED,
                gates.VERDICT_UNANCHORED, gates.VERDICT_INSUFFICIENT}) == 4
