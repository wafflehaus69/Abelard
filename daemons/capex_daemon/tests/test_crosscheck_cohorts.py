"""GAP2 P3 — the cross-check on fixed membership, and entries dated as breaks.

The published cross-check is a matched sum: whoever reports a quarter is in it
that quarter. Read as CURRENCY that is correct. Read as a LEVEL — "the supplier
side is running at X% of hyperscaler capex", which is how it is read — it is a
level compared across a changing roster.

Live, it has already misled. Between 2025Q2 and 2025Q3 the published series rose
46.73% -> 51.08%. On the two names that span both quarters the ratio FELL, 46.73%
-> 45.26%. The whole 4.35pp "rise" was Micron arriving.
"""
import types

import pytest

from capex_daemon import snapshot


def _hyper(ttm, members=5):
    return types.SimpleNamespace(
        ttm=ttm, membership={q: ["h"] * members for q in ttm})


# Four quarters of each supplier's discrete dcrev, so TTM exists from the fourth.
def _q(start_year, n, value):
    return {"{}Q{}".format(start_year + (i // 4), i % 4 + 1): value
            for i in range(n)}


def test_a_cohort_is_built_per_entry_and_named_for_its_size():
    members = {"A": _q(2024, 8, 10.0), "B": _q(2025, 4, 5.0)}
    hyper = _hyper({"{}Q{}".format(2024 + i // 4, i % 4 + 1): 100.0 for i in range(8)})
    out = snapshot.crosscheck_cohorts(members, hyper)
    labels = [c["label"] for c in out["cohorts"]]
    assert labels == ["single-name", "two-name"]
    assert out["cohorts"][1]["names"] == ["A", "B"]
    assert out["cohorts"][0]["is_crosscheck"] is False
    assert out["cohorts"][1]["is_crosscheck"] is True


def test_membership_is_constant_within_a_leg():
    """The point of the leg: every quarter in it has the same members, so a move
    in the line is a move in the ratio."""
    members = {"A": _q(2024, 8, 10.0), "B": _q(2025, 4, 5.0)}
    hyper = _hyper({"{}Q{}".format(2024 + i // 4, i % 4 + 1): 100.0 for i in range(8)})
    two = snapshot.crosscheck_cohorts(members, hyper)["cohorts"][1]
    assert all(r["dc"] == pytest.approx(60.0) for r in two["series"])


def test_an_entry_is_dated_and_its_step_measured_both_ways():
    """The only comparison that isolates the entrant is the same quarter read
    with and without it."""
    members = {"A": _q(2024, 8, 10.0), "B": _q(2025, 4, 5.0)}
    hyper = _hyper({"{}Q{}".format(2024 + i // 4, i % 4 + 1): 100.0 for i in range(8)})
    out = snapshot.crosscheck_cohorts(members, hyper)
    br = [b for b in out["breaks"] if b["entrant"] == "B"]
    assert len(br) == 1
    b = br[0]
    assert b["q"] == "2025Q4"                       # B's first full TTM quarter
    assert b["without"] == pytest.approx(0.40)      # A alone: 40/100
    assert b["with"] == pytest.approx(0.60)         # A+B: 60/100
    assert b["step_pp"] == pytest.approx(20.0)


def test_a_falling_ratio_can_hide_under_a_rising_published_line():
    """The live shape, in miniature: the entrant is large enough that the
    matched series rises while the constant-membership ratio falls."""
    a = _q(2024, 8, 10.0)
    a["2025Q4"] = 8.0                               # A weakens
    members = {"A": a, "B": _q(2025, 4, 6.0)}
    hyper = _hyper({"{}Q{}".format(2024 + i // 4, i % 4 + 1): 100.0 for i in range(8)})
    out = snapshot.crosscheck_cohorts(members, hyper)
    one = {r["q"]: r["ratio"] for r in out["cohorts"][0]["series"]}
    b = [x for x in out["breaks"] if x["entrant"] == "B"][0]
    assert one["2025Q4"] < one["2025Q3"]            # the ratio itself fell
    assert b["with"] > one["2025Q3"]                # the matched line rose


def test_a_leg_records_its_own_range_and_never_a_band():
    """E8: a range is an observation. Registering a band is Mando's ruling."""
    members = {"A": _q(2024, 8, 10.0)}
    hyper = _hyper({"{}Q{}".format(2024 + i // 4, i % 4 + 1): 100.0 for i in range(8)})
    c = snapshot.crosscheck_cohorts(members, hyper)["cohorts"][0]
    assert "min_ratio" in c and "max_ratio" in c
    assert "band" not in c


def test_a_changing_denominator_is_flagged_on_the_leg():
    """Fixed membership in the NUMERATOR does not fix the denominator. A leg
    whose capex membership moved says so rather than reading as clean."""
    members = {"A": _q(2024, 8, 10.0)}
    ttm = {"{}Q{}".format(2024 + i // 4, i % 4 + 1): 100.0 for i in range(8)}
    hyper = types.SimpleNamespace(
        ttm=ttm, membership={q: (["h"] * (5 if q != "2025Q4" else 4)) for q in ttm})
    c = snapshot.crosscheck_cohorts(members, hyper)["cohorts"][0]
    assert c["capex_members_changed"] is True


def test_no_hyperscaler_denominator_yields_nothing_rather_than_a_ratio():
    assert snapshot.crosscheck_cohorts({"A": _q(2024, 8, 10.0)}, None) == {}


def test_the_registered_band_travels_with_its_leg_and_only_its_leg():
    """Ruled 2026-09-28: 44-48% on AMD+NVDA from 2024Q3. The three-name leg
    carries no band, and every quarter since the band took effect is marked."""
    q = ["{}Q{}".format(2023 + i // 4, i % 4 + 1) for i in range(16)]
    # TTM numerators are four-quarter sums: (1.15 + 10.0) x 4 = 44.6 over 100.
    members = {"AMD": {x: 1.15 for x in q}, "NVDA": {x: 10.0 for x in q},
               "MU": {x: 1.0 for x in q[8:]}}
    hyper = _hyper({x: 100.0 for x in q})
    cohorts = {tuple(sorted(c["names"])): c
               for c in snapshot.crosscheck_cohorts(members, hyper)["cohorts"]}
    two = cohorts[("AMD", "NVDA")]
    assert two["band"]["low"] == 0.44 and two["band_position"] == "inside"
    assert all("band_position" in r for r in two["series"]
               if r["q"] >= "2024Q3")
    assert all("band_position" not in r for r in two["series"] if r["q"] < "2024Q3")
    assert "band" not in cohorts[("AMD", "MU", "NVDA")]
