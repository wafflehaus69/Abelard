"""P2 C6 — KEEL's $1.08B is debt. Ruling R1(c): confirm it is not double-counted.

R1(c) sent this check to the credit leg: "if it's debt outstanding it belongs in
debt stock and nowhere else". The answer to the question as asked is clean — the
credit leg measures ISSUANCE, a flow of proceeds, and KEEL's $1.079B is a stock
(long-term debt principal plus lease payments from a liquidity-risk maturity
table), so the two are not the same quantity and there is nothing to
double-count. C1 already removed it from the commitments leg, and the daemon
publishes no debt-stock series at all, so the figure now appears nowhere. That
is the correct place for it.

The check found something else while it was there, which is why it is worth
having run. KEEL tags the SAME H1-2026 raise twice:

  cash-flow statement   "Proceeds from long-term debt, net of transaction
                        costs 445,124"     -> ProceedsFromDebtNetOfIssuanceCosts
  Note 13, LT debt      "Issuance of long-term debt 458,650", transaction
  movement              costs on their own line
                                           -> ProceedsFromIssuanceOfLongTermDebt

458,650 - 13,526 = 445,124. One raise. The resolver summed them as distinct
instruments — $903,774K for a $458,650K raise — because the values are not
identical (not a double-tag) and they share ONE live period, below the
containment minimum. Verified in 10-Q 0001812477-26-000023, filed 2026-08-10.
"""
import pytest

from capex_daemon import issuance

NET = "ProceedsFromDebtNetOfIssuanceCosts"
GROSS = "ProceedsFromIssuanceOfLongTermDebt"

# KEEL's live figures, in dollars.
KEEL_NET = {("2026-01-01", "2026-06-30"): 445_124_000.0,
            ("2025-01-01", "2025-06-30"): 47_544_000.0}
KEEL_GROSS = {("2026-01-01", "2026-06-30"): 458_650_000.0,
              ("2025-01-01", "2025-12-31"): 689_306_000.0}


class _Res:
    def __init__(self, concepts):
        self.candidates_present = concepts


class _F:
    def __init__(self, ps, pe, v):
        self.period_start, self.period_end, self.value = ps, pe, v
        self.unit, self.filed = "USD", "2026-08-10"


def _indexed(series_by_concept):
    return {c: [_F(ps, pe, v) for (ps, pe), v in s.items()]
            for c, s in series_by_concept.items()}


def test_the_gross_and_net_tagging_of_one_raise_is_refused_not_summed():
    res = issuance.resolve_total(_indexed({NET: KEEL_NET, GROSS: KEEL_GROSS}),
                                 _Res([NET, GROSS]))
    assert res.is_refused
    assert "double-count" in res.detail


def test_the_sum_it_would_have_published_is_nearly_twice_the_raise():
    """What the refusal prevents, stated as the number a reader would have seen."""
    period = ("2026-01-01", "2026-06-30")
    assert KEEL_NET[period] + KEEL_GROSS[period] == pytest.approx(903_774_000.0)
    assert (KEEL_NET[period] + KEEL_GROSS[period]) / KEEL_GROSS[period] > 1.9


def test_a_net_of_costs_concept_alone_is_still_eligible():
    """The WULF ruling stands: net of ISSUANCE COSTS is a gross inflow. What is
    refused is summing it with its own gross counterpart, not the concept."""
    res = issuance.resolve_total(_indexed({NET: KEEL_NET}), _Res([NET]))
    assert not res.is_refused
    assert res.contributing == (NET,)


def test_two_genuinely_distinct_instruments_still_sum():
    """The rule must not swallow a real second facility. Here the net-tagged
    line EXCEEDS the gross-tagged one in a shared period, so they cannot be the
    same raise stated two ways."""
    bigger_net = {("2026-01-01", "2026-06-30"): 900_000_000.0}
    smaller_gross = {("2026-01-01", "2026-06-30"): 100_000_000.0}
    res = issuance.resolve_total(
        _indexed({NET: bigger_net, GROSS: smaller_gross}), _Res([NET, GROSS]))
    assert not res.is_refused
    assert set(res.contributing) == {NET, GROSS}


def test_identical_values_collapse_rather_than_refuse():
    """CIFR, live: it tags $167,113,000 as both ProceedsFromConvertibleDebt and
    ProceedsFromDebtNetOfIssuanceCosts in their one shared period. That is a
    double-TAG, and rule (a) handles it better than a refusal — collapse keeps
    the series, refusal withholds it. The first cut of this rule fired here and
    would have withheld a total that collapses cleanly."""
    same = {("2025-01-01", "2025-06-30"): 167_113_000.0}
    res = issuance.resolve_total(
        _indexed({NET: dict(same), "ProceedsFromConvertibleDebt": dict(same)}),
        _Res([NET, "ProceedsFromConvertibleDebt"]))
    assert not res.is_refused
    assert res.collapsed == (NET,)


def test_historical_overlap_alone_does_not_refuse_a_live_series():
    """Same scoping as every other pair rule: overlap outside the live window is
    history the era map owns, and must not withhold a current total."""
    old_net = {("2013-01-01", "2013-12-31"): 10.0}
    live_gross = {("2026-01-01", "2026-06-30"): 458_650_000.0}
    res = issuance.resolve_total(
        _indexed({NET: old_net, GROSS: live_gross}), _Res([NET, GROSS]))
    assert not res.is_refused


def test_the_pair_map_records_the_filing_language_that_put_it_there():
    """E23: a rule about presentation semantics cites the presentation."""
    grosses, why = issuance.COST_NET_RESTATEMENTS[NET]
    assert GROSS in grosses
    assert "445,124" in why and "458,650" in why and "0001812477-26-000023" in why
