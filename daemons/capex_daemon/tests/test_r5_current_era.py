"""Ruling R5 (Mando, 2026-09-28) — a collapsed double-tag keeps the CURRENT era.

CIFR is the case, verified in its filings. One cash-flow line changed its name:

  10-Q 9M 2025  (0001819989-25-000112)  "Proceeds from issuance of convertible
                notes, net of issuance costs 1,437,395"  -> ProceedsFromConvertibleDebt
  10-K FY 2025  (0001819989-26-000009)  "Proceeds from notes, net of issuance
                costs 3,145,829"                         -> ProceedsFromDebtNetOfIssuanceCosts
  10-Q H1 2026  (0001819989-26-000041)  same line, 2,766,437, with the H1 2025
                comparative 167,113 — the one period both names carry

The pair collapsed on that shared $167,113K, and the alphabet kept the
convertible name, which ENDS at 2025-09-30. Three of the four TTM quarters sat
under the dropped name. CIFR published $1.270B of TTM issuance; the filings say
$5,745,153K, and its credit-to-capex ratio went from 1.03 to 4.66.

Collapse now keeps the concept whose newest period is latest, and the older name
fills the periods the new one never reported — pooled at the FACT level, so a
fiscal year under the new name can subtract a nine-month YTD under the old one.
"""
import pytest

from capex_daemon import divergence, issuance
from capex_daemon.facts_api import ApiFact

OLD = "ProceedsFromConvertibleDebt"
NEW = "ProceedsFromDebtNetOfIssuanceCosts"


def _f(concept, start, end, value, filed):
    return ApiFact(concept, "us-gaap", "USD", value, start, end, None, "10-Q", filed, None)


# CIFR's live facts, in thousands -> dollars.
CIFR = {
    OLD: [_f(OLD, "2025-01-01", "2025-06-30", 167_113_000, "2025-08-01"),
          _f(OLD, "2025-01-01", "2025-09-30", 1_437_395_000, "2025-11-03")],
    NEW: [_f(NEW, "2025-01-01", "2025-03-31", 0, "2026-05-01"),
          _f(NEW, "2025-01-01", "2025-06-30", 167_113_000, "2026-08-01"),
          _f(NEW, "2025-01-01", "2025-12-31", 3_145_829_000, "2026-02-27"),
          _f(NEW, "2026-01-01", "2026-03-31", 1_969_780_000, "2026-05-01"),
          _f(NEW, "2026-01-01", "2026-06-30", 2_766_437_000, "2026-08-01")],
}


class _Res:
    def __init__(self, concepts):
        self.candidates_present = concepts


def _resolve(indexed):
    return issuance.resolve_total(indexed, _Res(sorted(indexed)))


def test_the_pair_still_collapses():
    """Identical in the one period both names carry — the same instrument."""
    res = _resolve(CIFR)
    assert not res.is_refused
    assert len(res.collapsed) == 1


def test_the_current_era_is_kept_not_the_alphabetically_first():
    res = _resolve(CIFR)
    assert res.contributing == (NEW,)
    assert res.collapsed == (OLD,)
    assert res.eras == {NEW: (OLD,)}


def test_the_old_name_fills_the_periods_the_new_one_never_reported():
    """Q4 2025 exists only as FY (new name) minus 9M (old name)."""
    res = _resolve(CIFR)
    merged = divergence._merged_issuance(CIFR, res.contributing, eras=res.eras)
    assert merged["2025-09-30"] == pytest.approx(1_270_282_000)   # 9M old - 6M
    assert merged["2025-12-31"] == pytest.approx(1_708_434_000)   # FY new - 9M old
    assert merged["2026-03-31"] == pytest.approx(1_969_780_000)
    assert merged["2026-06-30"] == pytest.approx(796_657_000)


def test_the_ttm_is_what_the_filings_say():
    res = _resolve(CIFR)
    merged = divergence._merged_issuance(CIFR, res.contributing, eras=res.eras)
    ttm = sum(merged[q] for q in ("2025-09-30", "2025-12-31", "2026-03-31", "2026-06-30"))
    assert ttm == pytest.approx(5_745_153_000)


def test_what_the_alphabet_published():
    """The number R5 retires, kept so the size of the correction is on record:
    only the convertible name had a fact in the window, so one quarter of four
    stood in for the year."""
    merged = divergence._merged_issuance(CIFR, (OLD,))
    window = [merged.get(q) for q in ("2025-09-30", "2025-12-31", "2026-03-31", "2026-06-30")]
    assert sum(v for v in window if v) == pytest.approx(1_270_282_000)


def test_a_simultaneous_double_tag_still_resolves_alphabetically():
    """No era to prefer when both names end on the same period."""
    a = [_f("ProceedsFromIssuanceOfLongTermDebt", "2026-01-01", "2026-06-30", 5e8, "2026-08-01")]
    b = [_f("ProceedsFromNotesPayable", "2026-01-01", "2026-06-30", 5e8, "2026-08-01")]
    res = _resolve({"ProceedsFromIssuanceOfLongTermDebt": a, "ProceedsFromNotesPayable": b})
    assert res.contributing == ("ProceedsFromIssuanceOfLongTermDebt",)


def test_the_current_era_wins_a_period_both_names_report():
    """On the one shared period the two agree, but if a restatement ever made
    them differ, the name the issuer uses NOW is the authority."""
    old = [_f(OLD, "2025-01-01", "2025-06-30", 100, "2025-08-01")]
    new = [_f(NEW, "2025-01-01", "2025-06-30", 100, "2026-08-01"),
           _f(NEW, "2025-01-01", "2025-12-31", 300, "2026-02-27")]
    pooled = divergence._era_pooled({OLD: old, NEW: new}, NEW, (OLD,))
    shared = [f for f in pooled if f.period_end == "2025-06-30"]
    assert len(shared) == 1 and shared[0].concept == NEW
