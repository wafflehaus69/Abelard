"""PS-1 Phase 2 — reconstruction and detector, against RECORDED VENDOR FIXTURES.

The synthetic tests in ``test_prices_writer.py`` prove the logic. These prove it
against what the vendor actually served, captured 2026-09-02 and committed as
fixtures so the MNST case stays reproducible after Yahoo repairs (or further
mangles) its own series.

Three names, chosen because they fail differently:

* **AAPL** — a clean 4:1 in 2020. The control. Anything that flags this is
  broken, and an early draft of the detector did exactly that.
* **MNST** — declares a 2:1 effective 2026-08-11 and applies it to only 6 of 21
  pre-split sessions. Must be caught and quarantined, never repaired.
* **MRNA** — a +177% single session with NO declared action. Must be flagged
  ``unknown``, never labelled a split we did not see declared.

Each carries a hand-verified 5-row check (order 2.4).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from abelard_common.prices import reconstruct as R
from abelard_common.prices.vendor import YahooVendor

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str):
    vendor = YahooVendor()
    body = json.loads((FIXTURES / name).read_text())
    symbol = name.split("_")[0].upper()
    return vendor.parse(symbol, body)


def pipeline(series):
    raw = R.reconstruct(series.bars, series.splits)
    anomalies = R.detect_anomalies(series.bars, series.splits)
    span = R.quarantine_span(anomalies, [b.date for b in series.bars], series.splits)
    return R.apply_quarantine(raw, span), anomalies, span


def by_date(bars):
    return {b.date: b for b in bars}


# ------------------------------------------------------- AAPL: the control --

def test_aapl_clean_split_is_not_flagged():
    s = load("aapl_2020_split.json")
    assert [(x.effective_date, x.ratio) for x in s.splits] == [("2020-08-31", 4.0)]
    bars, anomalies, span = pipeline(s)
    assert anomalies == [], "a correctly-served split must never be flagged"
    assert span == set()
    assert all(b.status == "ok" for b in bars)


def test_aapl_raw_recovers_the_true_traded_price():
    """5-ROW HAND CHECK. AAPL traded near $500 before the 2020-08-31 4:1 and
    near $125 after. The vendor serves ~$125 on both sides because its close is
    retro-split-adjusted; reconstruction must undo that.

        date        vendor close   x factor   = raw      actual traded
        2020-08-20      118.28       x4         473.10     ~473
        2020-08-21      124.37       x4         497.48     ~497
        2020-08-24      125.86       x4         503.43     ~503
        2020-08-31      129.04       x1         129.04     ~129  (split day)
        2020-09-01      134.18       x1         134.18     ~134
    """
    s = load("aapl_2020_split.json")
    raw = by_date(R.reconstruct(s.bars, s.splits))
    assert raw["2020-08-20"].close == pytest.approx(473.10, abs=0.01)
    assert raw["2020-08-21"].close == pytest.approx(497.48, abs=0.01)
    assert raw["2020-08-24"].close == pytest.approx(503.43, abs=0.01)
    assert raw["2020-08-31"].close == pytest.approx(129.04, abs=0.01)
    assert raw["2020-09-01"].close == pytest.approx(134.18, abs=0.01)


def test_aapl_volume_carries_the_inverse_factor():
    """The vendor multiplies historical volume by the ratio, so reconstruction
    divides. 2020-08-21's 338,054,800 adjusted shares is 84,513,700 real ones."""
    s = load("aapl_2020_split.json")
    raw = by_date(R.reconstruct(s.bars, s.splits))
    assert raw["2020-08-21"].volume == 84_513_700
    assert raw["2020-09-01"].volume == 151_948_100      # post-split, unchanged


def test_aapl_adjusted_view_returns_match_the_vendors():
    """The strongest available check on the factor series: our adjusted closes
    and the vendor's ``adjclose`` should differ only by a CONSTANT (dividends
    declared outside the fetch window), which means identical returns."""
    s = load("aapl_2020_split.json")
    bars, _, _ = pipeline(s)
    ours = R.adjusted_closes(bars, s.dividends, s.splits)
    ratios = [ours[d] / s.vendor_adjclose[d] for d in sorted(ours)]
    assert max(ratios) - min(ratios) < 1e-6, "returns must agree with the vendor"


# ---------------------------------------------------- MNST: the corrupt one --

def test_mnst_partial_adjustment_is_caught():
    s = load("mnst_2026_corrupt.json")
    assert [(x.effective_date, x.ratio) for x in s.splits] == [("2026-08-11", 2.0)]
    bars, anomalies, span = pipeline(s)
    assert len(anomalies) == 7
    assert {a.kind for a in anomalies} == {"vendor_corruption"}
    # Every flagged ratio is the declared 2.0 or its inverse, with the session's
    # own move riding on top -- which is why the match is on the residual.
    for a in anomalies:
        assert (R._ratio_matches(a.implied_ratio, 2.0)
                or R._ratio_matches(a.implied_ratio, 0.5)), a


def test_mnst_5_row_hand_check():
    """5-ROW HAND CHECK. Yahoo declares one 2:1 effective 2026-08-11. If honoured,
    every close before that date would be on the halved scale. It is not:

        date        vendor close   scale        verdict
        2026-07-17      97.50      PRE-split    not adjusted
        2026-07-20      47.72      post-split   adjusted      <- flip
        2026-07-23      93.56      PRE-split    not adjusted  <- flip back
        2026-08-11      45.53      post-split   correct from here on
        2026-08-12      45.98      post-split   correct

    Six of the twenty-one pre-split sessions are halved and fifteen are not, so
    no scale can be trusted before 2026-08-11.
    """
    s = load("mnst_2026_corrupt.json")
    vendor = {b.date: b.close for b in s.bars}
    assert vendor["2026-07-17"] == pytest.approx(97.50, abs=0.01)
    assert vendor["2026-07-20"] == pytest.approx(47.72, abs=0.01)
    assert vendor["2026-07-23"] == pytest.approx(93.56, abs=0.01)
    assert vendor["2026-08-11"] == pytest.approx(45.53, abs=0.01)
    assert vendor["2026-08-12"] == pytest.approx(45.98, abs=0.01)

    bars, _, span = pipeline(s)
    st = {b.date: b.status for b in bars}
    for d in ("2026-07-17", "2026-07-20", "2026-07-23", "2026-08-11"):
        assert st[d] == "quarantined", d
    assert st["2026-08-12"] == "ok", "the clean post-split window survives"


def test_mnst_quarantine_covers_the_whole_pre_split_window():
    """A uniformly mis-scaled stretch contains no STEP, so a step detector alone
    would stamp it as fact. Because the split date itself is discontinuous, the
    quarantine extends back over the entire pre-split region."""
    s = load("mnst_2026_corrupt.json")
    bars, _, span = pipeline(s)
    assert min(span) == min(b.date for b in s.bars)
    assert max(span) == "2026-08-11"
    ok = sorted(b.date for b in bars if b.status == "ok")
    assert ok == sorted(d for d in (b.date for b in s.bars) if d > "2026-08-11")


def test_mnst_quarantined_rows_never_reach_the_view():
    s = load("mnst_2026_corrupt.json")
    bars, _, _ = pipeline(s)
    view = R.adjusted_closes(bars, s.dividends, s.splits)
    assert all(d > "2026-08-11" for d in view)


# ------------------------------------------------------ MRNA: the undiagnosed --

def test_mrna_unexplained_jump_is_flagged_unknown_not_a_split():
    """MRNA moves +177% in one session with no declared action. We flag it and
    quarantine it, but we do NOT invent a split to explain it -- labelling
    something a corporate action the vendor never declared is exactly the kind
    of fabrication this substrate exists to prevent."""
    s = load("mrna_2026.json")
    assert s.splits == [] and s.dividends == []
    bars, anomalies, span = pipeline(s)
    assert len(anomalies) == 1
    a = anomalies[0]
    assert a.kind == "unknown"
    assert a.date == "2026-08-19"
    assert a.implied_ratio == pytest.approx(2.77, abs=0.01)
    assert "no declared action" in a.evidence["note"]


def test_mrna_5_row_hand_check():
    """5-ROW HAND CHECK.

        date        close    d/d       verdict
        2026-08-17   64.46    +1.8%    ordinary
        2026-08-18   62.96    -2.3%    ordinary
        2026-08-19  174.38  +177.0%    IMPLAUSIBLE, no declared action
        2026-08-20  133.32   -23.5%    post-jump scale
        2026-08-21  145.13    +8.9%    post-jump scale

    Only the pair spanning the jump is quarantined; the other twelve sessions
    stay facts. Contrast MNST, where the corruption reaches the split boundary
    and the whole pre-split window has to go.
    """
    s = load("mrna_2026.json")
    v = {b.date: b.close for b in s.bars}
    assert v["2026-08-18"] == pytest.approx(62.96, abs=0.01)
    assert v["2026-08-19"] == pytest.approx(174.38, abs=0.01)
    bars, _, span = pipeline(s)
    assert span == {"2026-08-18", "2026-08-19"}
    assert sum(1 for b in bars if b.status == "ok") == 12


# ------------------------------------------------------------ pure functions --

def test_split_factor_is_forward_looking_only():
    splits = [R.Split("2020-08-31", 4.0), R.Split("2014-06-09", 7.0)]
    assert R.split_factor("2013-01-01", splits) == 28.0
    assert R.split_factor("2015-01-01", splits) == 4.0
    assert R.split_factor("2020-08-31", splits) == 1.0   # on the day: already raw
    assert R.split_factor("2021-01-01", splits) == 1.0


def test_dividend_factor_is_crsp():
    raw = {"2026-05-25": 100.0, "2026-05-26": 98.0}
    f = R.dividend_factors(raw, [R.Dividend("2026-05-26", 2.0)])
    assert f["2026-05-26"] == pytest.approx(0.98)


def test_dividend_with_no_prior_session_is_skipped_not_guessed():
    """Guessing the prior close would fabricate the number the factor is
    entirely made of."""
    raw = {"2026-05-26": 98.0}
    assert R.dividend_factors(raw, [R.Dividend("2026-05-20", 2.0)]) == {}


def test_fact_changes_are_relative_not_absolute():
    held = {"2026-01-02": 100.0, "2026-01-03": 200.0}
    assert R.fact_changes(held, {"2026-01-02": 100.0 + 1e-12}) == []
    changed = R.fact_changes(held, {"2026-01-03": 200.5})
    assert [c.date for c in changed] == ["2026-01-03"]
    assert "does not change" in changed[0].message()


def test_fact_changes_ignores_dates_not_held():
    assert R.fact_changes({"2026-01-02": 1.0}, {"2026-01-09": 5.0}) == []


def test_reconstruct_preserves_vendor_nulls():
    bars = [R.Bar("2026-01-02", close=10.0), R.Bar("2026-01-03", close=None)]
    out = R.reconstruct(bars, [])
    assert out[1].status == "vendor_null" and out[1].close is None


def test_quarantine_keeps_the_values_as_evidence():
    bars = [R.RawBar("2026-01-02", 1, 1, 1, 10.0, 5, "ok")]
    out = R.apply_quarantine(bars, {"2026-01-02"})
    assert out[0].status == "quarantined" and out[0].close == 10.0
