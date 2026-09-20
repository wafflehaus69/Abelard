"""A sub-penny vendor gap is a convention, not a disputed fact.

The verification sweep compared closes on ``CROSS_VENDOR_EPS`` alone — a
RELATIVE 1e-6, sized for Yahoo's float32-widened closes. Yahoo and Tiingo differ
by a fixed half cent, which that epsilon cannot express: half a cent is 3.0e-4 of
a $16.69 close and 1.0e-5 of a $478.71 one.

So the sweep drafted a correction every night. Measured on the live store
2026-09-19: 67 draft rows outstanding, and in all 67 Yahoo sat exactly half a
cent above Tiingo — same sign every time, magnitudes 0.004973..0.005017, the
spread being float32 noise on Yahoo's side alone. One rounding convention, not
67 broken sessions. Signing them would have written a vendor preference into the
store as 67 human-attested facts while the sweep minted more each night.

Numbers below are taken from those drafts verbatim.
"""

from __future__ import annotations

import pytest

from abelard_common.prices.verify import _prices_agree, PRICE_TOLERANCE


# (ticker, yahoo_held, tiingo_verifier) — verbatim from the standing drafts.
REAL_DRAFTS = [
    ("LIN",  478.7099914550781, 478.705),
    ("LULU", 119.48000335693359, 119.475),
    ("HBAN", 16.690000534057617, 16.685),
    ("IDXX", 539.3699951171875, 539.365),
    ("GEN",  23.930000305175781, 23.925),
    ("INTC", 96.690002441406250, 96.685),
    ("FAST", 51.139999389648438, 51.135),
]


@pytest.mark.parametrize("ticker,yahoo,tiingo", REAL_DRAFTS)
def test_the_half_cent_convention_gap_is_agreement(ticker, yahoo, tiingo):
    """THE regression. Every standing draft row must read as agreement."""
    assert _prices_agree(tiingo, yahoo), (
        "%s: %r vs %r is %.6f apart — under a cent, so a convention"
        % (ticker, yahoo, tiingo, abs(yahoo - tiingo)))


def test_every_draft_row_was_the_same_sign_and_size():
    """Not a test of the fix but of the CLAIM behind it: if these gaps were real
    errors they would scatter. They do not — one direction, one magnitude."""
    diffs = [y - t for _tk, y, t in REAL_DRAFTS]
    assert all(d > 0 for d in diffs), "Yahoo is always the higher one"
    assert max(diffs) - min(diffs) < 1e-4, (
        "all within float32 noise of half a cent: %r" % (diffs,))


def test_a_full_cent_is_still_a_disagreement():
    """The tolerance is HALF a cent, so a whole one is a real disagreement.

    This is the boundary a 0.01 tolerance got wrong: 50.01 - 50.00 is
    0.009999999999990905 in binary floating point, so `< 0.01` called a genuine
    one-cent error agreement. Sitting the threshold ON the smallest real
    disagreement could only fail that way."""
    assert not _prices_agree(100.00, 100.01)
    assert not _prices_agree(50.00, 50.01)
    assert not _prices_agree(2.00, 2.01)


def test_a_real_error_is_still_caught():
    assert not _prices_agree(94.16, 941.60)     # dropped decimal
    assert not _prices_agree(478.71, 478.61)    # transposed digit
    assert not _prices_agree(16.69, 16.19)      # half-dollar out


def test_float32_widening_still_agrees():
    """The behaviour CROSS_VENDOR_EPS existed for must survive — a float32 close
    widened to float64 is the same number, not a disagreement."""
    assert _prices_agree(94.16, 94.16000366210938)


def test_the_absolute_gate_does_not_swallow_a_cheap_stock_error():
    """A penny on a $2 stock is 50 bp — real, and still caught. The absolute
    floor is about the DENOMINATION of the quote, not about size."""
    assert not _prices_agree(2.00, 2.01)
    assert not _prices_agree(2.00, 2.02)


def test_the_tolerance_sits_between_convention_and_the_smallest_real_error():
    """Half a cent is the most that nearest-cent rounding can move a price; a
    penny is the least two vendors can genuinely differ by. The threshold has to
    live strictly between them, with room for float32 noise on the low side."""
    assert 0.005 < PRICE_TOLERANCE < 0.01
    assert _prices_agree(50.000, 50.005)        # convention
    assert not _prices_agree(50.000, 50.0075)   # neither convention nor noise
